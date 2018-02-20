#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'acao_meter/modbus'

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
        amqp:,
        amqp_chan:,
        exchange:,
        meters:,
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

    @amqp = amqp
    @amqp_chan = amqp_chan
    @exchange = exchange

    @meters = meters

    actor_initialize(**args)
  end

  def actor_boot
    @state = :disconnected

    @address_selector = AddressSelector.new(
      name: @host, service: @port, socktype: :STREAM,
      family: (@ipv6 ? (@ipv4 ? nil : Socket::PF_INET6) : Socket::PF_INET),
    )

    @vars = {
      voltage:         { idx: 0x0000, },
      current:         { idx: 0x0006, },
      power:           { idx: 0x000C, },
      app_power:       { idx: 0x0012, },
      rea_power:       { idx: 0x0018, },
      power_factor:    { idx: 0x001E, },
      frequency:       { idx: 0x0046, },
      imported_energy: { idx: 0x0048, },
      exported_energy: { idx: 0x004A, },
      total_energy:    { idx: 0x0156, },
    }

    @meters_iterator = @meters.each
    @vars_iterator = @vars.each

    @last_cycle = Time.now

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

    @poll_timer = actor_set_timer(delay: 2.0, start: false) do
      poll_timeout
    end

    change_state! :ready

    run_poll_queue
  end

  def connection_closed!(cause: nil)
    log.warn("Connection closed in state #{@state}: #{cause}")

    if @poll_timer
      @poll_timer.stop!
      @poll_timer = nil
    end

    if @cycle_timer
      @cycle_timer.stop!
      @cycle_timer = nil
    end

    if @inter_request_timer
      @inter_request_timer.stop!
      @inter_request_timer = nil
    end

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

    if @out_buffer.empty?
      actor_epoll.mod(@socket, AM::Epoll::IN)

      out_buffer_empty!
    else
      actor_epoll.mod(@socket, AM::Epoll::IN | AM::Epoll::OUT)
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
            connection_closed!(cause: e)
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
      connection_closed!(cause: e)
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

    next_meter if !@current_meter

    begin
      ( @current_var_symbol, @current_var ) = @vars_iterator.next
    rescue StopIteration
      meter_completed

      return
    end

    @poll_current_req = ModBus::Req.new(address: @current_meter[:bus_address], function: 0x04,
                                        range: (@current_var[:idx]..(@current_var[:idx]+1)))
    send_frame(@poll_current_req.to_net)

    @poll_timer.start!
  end

  def poll_timeout
    if @current_meter && @current_var
      log.info "Timeout bus_addr=#{@current_meter[:bus_address]} idx=#{@current_var[:idx]}"
    else
      log.err "Unexpected shit happened. cm=#{@current_meter} cv=#{@current_var}"
    end

    @in_buffer.clear
    run_poll_queue
  end

  def next_meter
    ( @current_meter_uuid, @current_meter ) = @meters_iterator.next
    @current_meter_values = {}
  end

  def meter_completed
    @amqp.tell AM::AMQP::MsgPublish.new(
      channel_id: @amqp_chan,
      exchange: @exchange,
      payload: @current_meter_values.to_json,
      routing_key: @current_meter_uuid,
      persistent: false,
      mandatory: false,
      headers: {
        'Content-type': 'application/json',
        type: 'METER_UPDATE',
      },
    )

    @vars_iterator.rewind
    @current_meter_values = {}

    begin
      next_meter
      run_poll_queue
    rescue StopIteration
      @meters_iterator.rewind
      @current_meter = nil

      cycle_completed
    end
  end

  def cycle_completed
    time_to_wait = [ 120.seconds - (Time.now - @last_cycle), 1 ].max

    @cycle_timer = delay(time_to_wait) do
      @last_cycle = Time.now
      run_poll_queue
    end
  end

  def receive_frame(frame)
    if !@current_meter
      log.warn "Received frame with current_meter=nil"
      return
    end

    debug_data { "RX bus_addr=#{@current_meter[:bus_address]} #{@current_var_symbol}=#{frame.get_f32(0)}" }

    @current_meter_values[@current_var_symbol] = frame.get_f32(0)

    @poll_timer.stop!

    @inter_request_timer = delay(0.25.seconds) do
      run_poll_queue
    end
  end

  def send_frame(frame)
    debug_serial { "TX: #{frame.unpack('H*')}" }

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
  def debug_data(&block) ; log.debug block.call if @debug_data ; end
  def debug_serial(&block) ; log.debug block.call if @debug_serial ; end
  def debug_serial_raw(&block) ; log.debug block.call if @debug_serial_raw ; end
end

end
