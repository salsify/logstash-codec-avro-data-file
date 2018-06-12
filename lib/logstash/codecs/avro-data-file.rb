# encoding: utf-8

require 'avro'
require 'logstash/codecs/base'
require 'logstash/namespace'
require 'tmpdir'

# == Logstash Codec - Avro Data File
# 
# This plugin is used to process logstash events that represent
# Avro data files, like the S3 input can produce.
#
# ==== Options
#
# - ``temporary_directory`` - optional. Specifies a directory to store
#   temporary files in
# - ``decorate_events`` - will add avro schema metadata to the events.
#
# ==== Usage
#
# input {
#   stdin { codec => 'avro-data-file' }
# }
#
class LogStash::Codecs::AvroDataFile < LogStash::Codecs::Base

  config_name 'avro-data-file'

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash/avro
  config :temporary_directory, validate: :string, default: File.join(Dir.tmpdir, 'logstash', 'avro')
  config :decorate_events, validate: :boolean, default: false

  def register
    require 'fileutils'
    FileUtils.mkdir_p(temporary_directory) unless Dir.exist?(temporary_directory)
    reset
  end

  def decode(data)
    merge(data)
  end

  def flush
    tempfile.flush
    return unless block_given?

    Avro::DataFile.open(tempfile.path, 'r') do |reader|
      schema = reader.datum_reader.writers_schema
      reader.each do |avro_message|
        event = LogStash::Event.new(avro_message)
        decorate_event(event, schema) if decorate_events?
        yield event
      end
    end
  rescue => e
    puts e
    @logger.error('Avro parse error', error: e)
  ensure
    reset
  end

  def encode(_event)
    raise 'Not implemented'
  end

  private

  attr_accessor :tempfile
  attr_reader :temporary_directory

  def merge(bytes)
    tempfile.write(bytes)
  end

  def reset
    unless tempfile.nil?
      begin
        File.unlink(tempfile.path)
        tempfile.close
      rescue Errno::ENOENT # rubocop:disable Lint/HandleExceptions
      end
    end
    self.tempfile = Tempfile.create('', temporary_directory)
  end

  def decorate_events?
    @decorate_events
  end

  def decorate_event(event, schema)
    event.set('[@metadata][avro][type]', schema.type)
    if schema.is_a?(Avro::Schema::NamedSchema)
      event.set('[@metadata][avro][name]', schema.name)
      event.set('[@metadata][avro][namespace]', schema.namespace)
    end
  end
end
