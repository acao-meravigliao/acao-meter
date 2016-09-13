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
        keep_connected: false,
        connect_later: false,
        reconnect_delay: 3,
        reconnect_backoff: 2,
        reconnect_max_delay: 60,
        debug: 0,
        debug_data: false,
        debug_serial: false,
        debug_serial_raw: false,
        **args)

    @host = host
    @port = port
    @ipv4 = ipv4
    @ipv6 = ipv6
    @connect_later = connect_later
    @keep_connected = keep_connected
    @reconnect_delay = reconnect_delay
    @reconnect_backoff = reconnect_backoff
    @reconnect_max_delay = reconnect_max_delay

    @debug = debug
    @debug_data = debug_data
    @debug_serial = debug_serial
    @debug_serial_raw = debug_serial_raw

    actor_initialize(**args)
  end

  def actor_boot
    @state = :disconnected

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

    @vars_iterator = @vars.each

    start_connection! unless @connect_later
  end

  def init_state!
    @out_buffer = VihaiIoBuffer.new
    @in_buffer = VihaiIoBuffer.new

    @socket = nil
  end

  def start_connection!
    init_state!

    case @state
    when :disconnected, :connection_failure
      @address_selector.reset_attempts_counters!
    else
      @reconnect_channels = @saved_channels
      @saved_channels = nil
    end

    change_state!(:connect_resolving)

    debug1 { "Resolving to connect..." }

    @resolver = @address_selector.start_resolver(report_to: self)
  end

  def resolving_complete!
    debug2 { "Resolution complete, address selector state:\n" + @address_selector.inspect }

    if @address_selector.empty?
      reconnect_delay = 5.seconds

      debug1 { "No address available, we should wait #{'%.3f' % reconnect_delay} seconds" }

      change_state!(:connect_waiting)
      @connect_delay_timer = delay(reconnect_delay) do
        start_connection!
      end

      return
    end

    if @address_selector.current.last_attempt
      debug2 { "This is not the first attempt" }

      reconnect_delay = [ @reconnect_delay * ((@reconnect_backoff ** @address_selector.current.attempt_count) - 1),
                          @reconnect_max_delay ].min
      time_from_last = Time.now - @address_selector.current.last_attempt

      debug3 { "Should we wait #{time_from_last} < #{reconnect_delay} ?" }

      if time_from_last < reconnect_delay
        actual_delay = reconnect_delay - time_from_last
        debug2 { "This is not the first attempt, we should wait #{'%.3f' % actual_delay} seconds" }

        change_state!(:connect_waiting)
        @connect_delay_timer = delay(actual_delay) do
          do_connect!
        end
      else
        do_connect!
      end
    else
      do_connect!
    end
  end

  class NoAddressesFound < StandardError ; end

  def do_connect!
    change_state!(:connecting)

    debug2 { "Picking an address..." }

    if !@address_selector.current
      connection_closed!(cause: NoAddressesFound.new)
      return
    end

    debug1 { "Connecting to '#{@address_selector.current.inspect_addr}'" }

    @socket = Socket.new(@address_selector.current.addrinfo.ipv6? ? Socket::AF_INET6 : Socket::AF_INET, Socket::SOCK_STREAM, 0)
    @socket.bind(Socket.pack_sockaddr_in(0, @bind_to)) if @bind_to
    @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    #@socket.setsockopt(Socket::IPPROTO_TCP, 18, [ 3000 ].pack("l"))

    actor_epoll.add(@socket, AM::Epoll::IN)

    @address_selector.attempting!

    begin
      @socket.connect_nonblock(@address_selector.current.addrinfo)
    rescue IO::WaitWritable
      actor_epoll.mod(@socket, AM::Epoll::IN | AM::Epoll::OUT)
    rescue SystemCallError, IOError => e
      connection_closed!(cause: e)
      return
    else
      connected!
    end
  end

  def connected!
    @address_selector.success!

    @poll_current_idx = 0

    change_state! :ready

    run_poll_queue

#    @poller = now_and_every(5.seconds) do
#      @req = ModBus::Req.new(address: 2, function: 0x04, range: (0x0000..0x0001))
#
#      send_frame(@req.to_net)
#    end
  end

  def connection_closed!(cause: nil)
    log.warn("Connection closed in state #{@state}: #{cause}")

    if @resolver
      @resolver.kill
      @resolver = nil
    end

    if @connect_delay_timer
      @connect_delay_timer.stop!
      @connect_delay_timer = nil
    end

    if @socket
      actor_epoll.del(@socket)
      @socket.close
      @socket = nil
    end

    case @state
    when :connect_resolving, :connect_waiting, :connecting, :ready, :disconnecting_by_srv

      @address_selector.failure!

      if @address_selector.attempted_all?
        debug2 { "All addresses attempted, making connection requests fail" }

        @address_selector.reset_attempt_flags!

        @connect_requests.each { |x| actor_reply(x, MsgConnectFailure.new(cause: (@disconnecting_cause || cause))) }
        @connect_requests = []
      end

      @saved_channels = @channels.dup

      @channels.each do |channel_id, channel|
        channel.connection_closed!(
          permanent: !@keep_connected,
          reply_code: @disconnecting_cause ? @disconnecting_cause.code : 500,
          reply_text: @disconnecting_cause ? @disconnecting_cause.text : cause.to_s,
        )
      end

      if @keep_connected
        start_connection!
      else
        change_state!(:connection_failure)
      end

    when :disconnecting_by_us
      disconnected_by_us!
    else
      raise "Unexpected connection failure in state #{@state}"
    end
  end

  def flush_out_buffer!
    begin
      sent = @out_buffer.write_nonblock_to(@socket)
    rescue IO::WaitWritable
      actor_epoll.mod(@socket, AM::Epoll::IN | AM::Epoll::OUT)
    rescue SystemCallError, IOError => e
      connection_closed!(cause: e)
      return
    end

    if sent == 0
      actor_epoll.mod(@socket, AM::Epoll::IN | AM::Epoll::OUT)
    end

    if @out_buffer.empty?
      actor_epoll.mod(@socket, AM::Epoll::IN)

      out_buffer_empty!
    end
  end

  def out_buffer_empty!
  end
















  def actor_receive(events, io)
    case io
    when @socket
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

      # @socket may change as a consequence of receive_from_socket so we better check and desist in case
      return if io != @socket

      if (events & SleepyPenguin::Epoll::IN) != 0
        receive_from_socket(events, io)
      end
    else
      super
    end
  end

  def actor_handle(message)
    case message
    when AM::Actor::MsgExited
      case @state
      when :connect_resolving
        if message.sender == @resolver
          @address_selector.resolve_done!(message.exit_result)
          resolving_complete!
        end
      else
        # Ignore for now, report and error when resolution cancelling will be available
      end

    when AM::Actor::MsgCrashed
      case @state
      when :connect_resolving
        if message.sender == @resolver
          log.warn "Resolution failed: #{message.exception}"
          resolving_complete!
        end
      else
        # Ignore for now, report and error when resolution cancelling will be available
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

    (var_name , var) = @vars_iterator.next

    @poll_current_req = ModBus::Req.new(address: var[:addr], function: 0x04, range: (var[:idx]..(var[:idx]+1)))
    send_frame(@poll_current_req.to_net)

    @poll_timer = delay(2.seconds) do
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
    :connect_resolving,
    :connect_waiting,
    :connecting,
    :ready,
    :disconnecting_by_us,
    :disconnecting_by_srv,
    :connection_failure,
    :disconnected,
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
