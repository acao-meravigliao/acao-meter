#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'acao-meter/modbus'

module AcaoMeter

class Connection
  include AM::Actor

  def initialize(
        host:,
        port:,
        ipv4: true,
        ipv6: true,
        bind_to: nil,
        reconnect_delay: 3,
        reconnect_backoff: 2,
        reconnect_max_delay: 60,
        debug: 0,
        debug_data: false,
        debug_serial: false,
        debug_serial_raw: false)

    @host = host
    @port = port
    @ipv4 = ipv4
    @ipv6 = ipv6
    @reconnect_delay = reconnect_delay
    @reconnect_backoff = reconnect_backoff
    @reconnect_max_delay = reconnect_max_delay

    @debug = debug
    @debug_data = debug_data
    @debug_serial = debug_serial
    @debug_serial_raw = debug_serial_raw

    @state = :disconnected

    actor_initialize
  end

  def actor_boot

    @address_selector = AddressSelector.new(
      name: @host, service: @port, socktype: :STREAM,
      family: (@ipv6 ? (@ipv4 ? nil : Socket::PF_INET6) : Socket::PF_INET),
    )

    @vars = {
      voltage: { addr: 2, idx: 0x0000, },
      current: { addr: 2, idx: 0x0006, },
      power: { addr: 2, idx: 0x000C, },
      app_power: { addr: 2, idx: 0x0012, },
      power_factor: { addr: 2, idx: 0x001E, },
      frequency: { addr: 2, idx: 0x0046, },
      total_energy: { addr: 2, idx: 0x0201, },
      positive_energy: { addr: 2, idx: 0xF101, },
      reverse_energy: { addr: 2, idx: 0xF201, },
    }

    reset_state!

    connect!
  end

  def reset_state!
    @out_buffer = VihaiIoBuffer.new
    @in_buffer = VihaiIoBuffer.new

    @socket = nil
  end

  def init_socket!
    @address_selector.update!

    raise "XXX No address found" if !@address_selector.current

    debug2 { "Sorted address list:\n" + @address_selector.list.map { |x|
             "  #{x.addrinfo.inspect} - last_attempt=#{x.last_attempt}," +
             " last_success=#{x.last_success}, last_failure=#{x.last_failure}"
           }.join("\n") }
    debug1 { "Connecting to '#{@address_selector.current.inspect}'" }

    @socket = Socket.new(@address_selector.current.addrinfo.ipv6? ? Socket::AF_INET6 : Socket::AF_INET, Socket::SOCK_STREAM, 0)
    @socket.bind(Socket.pack_sockaddr_in(0, @bind_to)) if @bind_to
    @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

    actor_epoll.add(@socket, SleepyPenguin::Epoll::IN)
  end

  def connect!
    change_state! :connecting

    init_socket!

    @address_selector.attempting!

    begin
      @socket.connect_nonblock(@address_selector.current.addrinfo)
    rescue IO::WaitWritable
      actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN | SleepyPenguin::Epoll::OUT)
    rescue SystemCallError, IOError => e
      @address_selector.failure!
      connection_lost!(cause: e)
      return
    else
      connected!
    end
  end

  def connected!
    @address_selector.success!

    @socket.send(AMQ::Protocol::PREAMBLE, 0)

    change_state! :open

    @poll_current_idx = 0

    run_poll_queue

#    @poller = now_and_every(5.seconds) do
#      @req = ModBus::Req.new(address: 2, function: 0x04, range: (0x0000..0x0001))
#
#      send_frame(@req.to_net)
#    end
  end

  def connection_lost!(cause: nil)
    log.warn("CONNECTION LOST: #{cause}")

    connection_cleanup!

    attempt_reconnection!
  end

  def attempt_reconnection!
    if @state != :reconnecting
      @address_selector.reset_attempts_counters!

      @reconnect_started = Time.now

      debug1 { "Entering reconnect state at #{@reconnect_started}" }

      reset_state!

      change_state! :reconnecting
    end

    if @address_selector.current.last_attempt
      reconnect_delay = [ @reconnect_delay * (@reconnect_backoff ** @address_selector.current.attempt_count),
                          @reconnect_max_delay ].min

      time_from_last = Time.now - @address_selector.current.last_attempt

      debug2 { "Should we wait a grace period? #{time_from_last} < #{reconnect_delay} ?" }

      if time_from_last < reconnect_delay
        debug2 { "No, wait #{reconnect_delay - time_from_last} seconds" }

        delay(reconnect_delay - time_from_last) do
          reconnect!
        end
      else
        debug2 { 'Yes, reconnect immediately' }
        reconnect!
      end
    else
      reconnect!
    end
  end

  def reconnect!
    debug1 { "Reconnecting!" }

    init_socket!

    @address_selector.attempting!

    begin
      @socket.connect_nonblock(@address_selector.current.addrinfo)
    rescue IO::WaitWritable
      actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN | SleepyPenguin::Epoll::OUT)
    rescue SystemCallError => e
      @address_selector.failure!
      connection_lost!(cause: e)
      return
    else
      connected!
    end
  end

  def connection_cleanup!
    if @heartbeat_timeout_timer
      @heartbeat_timeout_timer.stop!
      @heartbeat_timeout_timer = nil
    end

    if @heartbeat_tx_timer
      @heartbeat_tx_timer.stop!
      @heartbeat_tx_timer = nil
    end

    if @socket
      actor_epoll.del @socket
      @socket.close
      @socket = nil
    end
  end

  def flush_out_buffer!
    debug_serial_raw { "RAW TX: #{@out_buffer.to_s.unpack('H*')}" }

    begin
      sent = @out_buffer.write_nonblock_to(@socket)
    rescue IO::WaitWritable
      actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN | SleepyPenguin::Epoll::OUT)
    rescue Errno::ECONNRESET => e
      if @state == :disconnecting_by_srv
        out_buffer_empty!
      else
        connection_lost!(cause: e)
      end

      return
    rescue SystemCallError, IOError => e
      connection_lost!(cause: e)
      return
    end

    if sent == 0
      actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN | SleepyPenguin::Epoll::OUT)
    end

    if @out_buffer.empty?
      actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN)

      out_buffer_empty!
    end
  end

  def out_buffer_empty!
  end

  def receive(events, io)
    case io
    when @socket
      if (events & SleepyPenguin::Epoll::IN) != 0
        receive_from_socket(events, io)
      end

      # @socket may change as a consequence of receive_from_socket so we better check and desist in case
      return if io != @socket

      if (events & SleepyPenguin::Epoll::OUT) != 0
        case @state
        when :connecting, :reconnecting
          begin
            @socket.connect_nonblock(@address_selector.current.addrinfo)
          rescue Errno::EISCONN
            actor_epoll.mod(@socket, SleepyPenguin::Epoll::IN)
            connected!
          rescue SystemCallError => e
            @address_selector.failure!
            connection_lost!(cause: e)
          end
        else
          flush_out_buffer!
        end
      end
    else
      super
    end
  end

  def receive_from_socket(events, io)
    begin
      data = @socket.recv_nonblock(16384)
      raise EOFError if !data || data.empty?
    rescue SystemCallError, IOError => e
      connection_lost!(cause: e)
      return
    end

    debug_serial_raw { "RAW RX: #{data.unpack('H*')}" }

    @in_buffer << data

    begin
      resp = ModBus::Resp.new_from_net(@in_buffer)
    rescue ModBus::Msg::Incomplete
    rescue ModBus::Msg::InvalidCRC
      log.info 'CRC Error'
    else
      receive_frame(resp)
    end
  end

  def run_poll_queue
    return if @poll_current_idx

    var = @vars[@poll_current_idx]

    @poll_current_req = ModBus::Req.new(address: var[:addr], function: 0x04, range: (var[:idx]..(var[:idx]+1)))
    send_frame(@poll_current_req.to_net)

    @poll_timer = after(2.seconds) do
      log.warn "Request addr=#{var[:addr]} idx=#{var[:idx]} timeout"
    end
  end

  def receive_frame(frame)
    log.warn(frame.get_f32(0))

    @poll_timer.stop!
  end

  def send_frame(frame)
    debug_serial { "TX: #{frame.unpack('H*')}" }

#    @heartbeat_tx_timer.reset! if @heartbeat_tx_timer

    @out_buffer << frame

    flush_out_buffer!
  end

  STATES = [
    :open,
    :connecting,
    :disconnected,
    :reconnecting_failure,
    :reconnecting,
    :connection_failure,
  ].freeze

  def change_state!(new_state)
    debug1 { "Connection change state from #{@state} to #{new_state}" }

    raise "Invalid State #{new_state}" if !STATES.include?(new_state)

    @state = new_state
  end

  def debug1(&block) ; log.debug block.call if @debug >= 1 ; end
  def debug2(&block) ; log.debug block.call if @debug >= 2 ; end
  def debug3(&block) ; log.debug block.call if @debug >= 3 ; end
  def debug_serial(&block) ; log.debug block.call if @debug_serial ; end
  def debug_serial_raw(&block) ; log.debug block.call if @debug_serial_raw ; end
end

end
