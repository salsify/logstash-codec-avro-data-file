# encoding: utf-8
require_relative '../spec_helper'
require 'logstash/codecs/avro-data-file'
require 'json'

describe LogStash::Codecs::AvroDataFile do
  let(:config) { {} }
  let(:codec) { described_class.new(config).tap(&:register) }
  let(:lines) { [] }
  let(:events) { [] }
  let(:expected_events) { [] }

  before do
    lines.each(&codec.method(:decode))
  end

  shared_examples "produces the correct output" do
    specify do
      expect(events.length).to eq expected_events.length
      events.each do |event| 
        expect(event).to be_an_instance_of(LogStash::Event)
      end
      event_hashes = events.map(&:to_hash).map do |event|
        event.except("@timestamp", "@version")
      end
      expected_hashes = expected_events.map(&:to_hash).map do |event|
        event.except("@timestamp", "@version")
      end

      expect(event_hashes).to eq expected_hashes
    end
  end

  describe "#decode" do

    context "without flushing" do
      let(:lines) { ['test', 'test2'] }

      include_examples "produces the correct output"
    end

    context "with a flush" do

      before do
        codec.flush do |event|
          events << event
        end
      end

      context "invalid data" do
        let(:lines) { ['test', 'test2'] }

        include_examples "produces the correct output"
      end

      context "valid data" do
        let(:schema) do
          {
            'type' => 'record',
            'name' => 'mock_schema',
            'namespace' => 'com.salsify.test',
            'fields' => [
              {
                'name' => 'id',
                'type' => 'long'
              }
            ]
          }.to_json
        end
        let(:avro_ids) { (0...100).to_a }
        let(:avro_messages) do
          avro_ids.map do |id|
            { 'id' => id }
          end
        end
        let(:tempfile) do
          Tempfile.new('restore-test')
        end
        let(:lines) do
          Avro::DataFile.open(tempfile.path, 'w', schema) do |writer|
            avro_messages.each do |message|
              writer << message
            end
          end
          File.open(tempfile.path, 'rb') do |file|
            file.to_a
          end
        end
        let(:expected_events) { avro_messages.map(&LogStash::Event.method(:new)) }

        include_examples "produces the correct output"
      end
    end
  end
end
