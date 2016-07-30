#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

module AcaoMeter

class Client
  def initialize(
    host:,
    port:,
    trace: 0,
    debug_data: false,
    debug_serial: false,
    debug_serial_raw: false,


    @address_selector = AddressSelector.new(
      name: @base_uri.hostname, service: @base_uri.port, socktype: :STREAM,
      family: (ipv6 ? (ipv4 ? nil : Socket::PF_INET6) : Socket::PF_INET),
    )

    reset_state!

    actor_initialize(actor_name: "#{@base_uri.host}:#{@base_uri.port}" , **args)
  end


  def init_socket!

# TODO IMPLEMENT SRV records for HTTP/1.2
    @address_selector.update!

    raise "XXX No address found" if !@address_selector.current

    trace2 { "Sorted address list:\n" + @address_selector.list.map { |x|
             "  #{x.addrinfo.inspect} - last_attempt=#{x.last_attempt}," +
             " last_success=#{x.last_success}, last_failure=#{x.last_failure}"
           }.join("\n") }
    trace1 { "Connecting to '#{@address_selector.current.inspect}'" }

    @socket = Socket.new(@address_selector.current.addrinfo.ipv6? ? Socket::AF_INET6 : Socket::AF_INET, Socket::SOCK_STREAM, 0)
    @socket.bind(Socket.pack_sockaddr_in(0, @bind_to)) if @bind_to
    @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
#    @socket.setsockopt(Socket::IPPROTO_TCP, 18, [ 3000 ].pack("l"))
    @actor_epoll.add(@socket, SleepyPenguin::Epoll::IN)
  end

  def receive(events, io)
    case io
    when @serialport
      data = @serialport.read_nonblock(65536)

      log.debug "Serial Raw: #{data.unpack('H*')}" if mycfg.debug_serial_raw

      if !data || data.empty?
        @actor_epoll.del(@socket)
        actor_exit
        return
      end

      receive_data(data)
    else
      super
    end
  end

  def receive_data(data)
    @buffer << data
    resp = ModBus::Resp.new_from_net(@buffer)
    log.debug "RX: #{@buffer.unpack('H*')}" if mycfg.debug_serial

    receive_packet(resp)
  rescue ModBus::Msg::Incomplete
    nil
  else
    @buffer = ''
  end

  def receive_packet(pkt)
    @humidity = pkt.payload[0] / 10.0
    @temperature = pkt.payload[1] / 10.0
    @dewpoint = pkt.payload[2] / 10.0
    @delta_t = pkt.payload[3] / 10.0

    log.debug "T=#{@temperature}°C RH=#{@humidity}% DP=#{@dewpoint}°C DT=#{@delta_t}°C" if mycfg.debug_data

    @amqp.tell AM::AMQP::MsgPublish.new(
      channel_id: @amqp_chan,
      exchange: mycfg.exchange,
      payload: {
        station_id: mycfg.station_name,
        sample_ts: Time.now,
        data: {
          humidity: @humidity,
          temperature: @temperature,
          dewpoint: @dewpoint,
          delta_t: @delta_t,
        }
      }.to_json,
      persistent: false,
      mandatory: false,
      routing_key: mycfg.station_name,
      headers: {
        'Content-type': 'application/json',
        type: 'WX_UPDATE',
      }
    )

  end
end

end
