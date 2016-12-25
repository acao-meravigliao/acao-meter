#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'acao_meter/version'
require 'acao_meter/task'

require 'acao_meter/connection'
require 'acao_meter/modbus'

module AcaoMeter

class App < Ygg::Agent::Base
  self.app_name = 'acao_meter'
  self.app_version = VERSION
  self.task_class = Task

  def prepare_default_config
    app_config_files << File.join(File.dirname(__FILE__), '..', 'config', 'acao_meter.conf')
    app_config_files << '/etc/yggdra/acao_meter.conf'
  end

  def prepare_options(o)
    o.on("--debug-data", "Logs decoded data") { |v| @config['acao_meter.debug_data'] = true }
    o.on("--debug-serial", "Logs serial lines") { |v| @config['acao_meter.debug_serial'] = true }
    o.on("--debug-serial-raw", "Logs serial bytes") { |v| @config['acao_meter.debug_serial_raw'] = true }

    super
  end

  def agent_boot
    @amqp.ask(AM::AMQP::MsgExchangeDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.exchange,
      type: :topic,
      durable: true,
      auto_delete: false,
    )).value

    mycfg.buses.each do |bus_name, bus|
      AcaoMeter::Connection.new(
        actor_name: "conn-#{bus[:address]}:#{bus[:port]}",
        host: bus[:address], port: bus[:port],
        amqp: @amqp,
        amqp_chan: @amqp_chan,
        exchange: mycfg.exchange,
        meters: bus[:meters],
        keep_connected: true,
        debug: 3,
        debug_data: mycfg.debug_data,
        debug_serial: mycfg.debug_serial,
        debug_serial_raw: mycfg.debug_serial_raw,
      )
    end
  end

end

end
