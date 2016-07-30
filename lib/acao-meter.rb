#
# Copyright (C) 2016-2016, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'acao-meter/version'
require 'acao-meter/task'

require 'acao-meter/connection'
require 'acao-meter/modbus'

module AcaoMeter

class App < Ygg::Agent::Base
  self.app_name = 'acao-meter'
  self.app_version = VERSION
  self.task_class = Task

  def prepare_default_config
    app_config_files << File.join(File.dirname(__FILE__), '..', 'config', 'acao-meter.conf')
    app_config_files << '/etc/yggdra/acao-meter.conf'
  end

  def prepare_options(o)
    o.on("--debug-data", "Logs decoded data") { |v| @config['acao-meter.debug_data'] = true }
    o.on("--debug-serial", "Logs serial lines") { |v| @config['acao-meter.debug_serial'] = true }
    o.on("--debug-serial-raw", "Logs serial bytes") { |v| @config['acao-meter.debug_serial_raw'] = true }

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

    AcaoMeter::Connection.new(host: '192.168.0.116', port: 8899,
      debug: 3,
      debug_data: mycfg.debug_data,
      debug_serial: mycfg.debug_serial,
      debug_serial_raw: mycfg.debug_serial_raw)
  end

end

end
