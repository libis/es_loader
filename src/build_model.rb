#encoding: UTF-8
$LOAD_PATH.unshift(File.expand_path('./lib', __dir__))

require 'logger'
require 'data_model_builder'

builder = DataModelBuilder.new(logger: Logger.new(STDOUT))

#pp builder::config.keys

builder.run
