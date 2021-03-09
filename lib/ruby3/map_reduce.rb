require 'benchmark'
require 'benchmark/memory'
require 'json'
require_relative 'map_reduce/text_generator'
require_relative 'map_reduce/text_slicer'

Warning[:experimental] = false

module Ruby3
  module MapReduce
    INPUT_DATA_FILE =  + "#{__dir__}/../../data/map_reduce/input_data.txt".freeze
    THREADS = 10
    WORD_COUNT = 250_000

    ## MAP: Count the number of occurrences of each word available in input data.
    # REDUCE: Compute the average repeating of all words available in input data
    def self.call
      text = load_data
      text_parts = split_data(text)
      ##pp text_parts
      
      word_counts_array = []
      Benchmark.bm(15) do |x|
        x.report('Plain Ruby:') { word_counts_array = mapping_data_plain_ruby(text_parts) }
        x.report('Green Threads:') { word_counts_array = mapping_data_green_threads(text_parts) }
        x.report('System Threads:') { word_counts_array = mapping_data_system_threads(text_parts) }
        if RUBY_VERSION.start_with?('3')
          x.report('Ractors:') { word_counts_array = mapping_data_ractors(text_parts) }
        end
      end
      Benchmark.memory do |x|
        x.report('Plain Ruby:') { word_counts_array = mapping_data_plain_ruby(text_parts) }
        x.report('Green Threads:') { word_counts_array = mapping_data_green_threads(text_parts) }
        x.report('System Threads:') { word_counts_array = mapping_data_system_threads(text_parts) }
        if RUBY_VERSION.start_with?('3')
          x.report('Ractors:') { word_counts_array = mapping_data_ractors(text_parts) }
        end
      end
      #pp word_counts_array

      word_counts = merge_data(word_counts_array)
      #pp word_counts.sort.to_h

      average_word_length = reduce_data(word_counts)
      pp average_word_length
    end

    def self.load_data
      unless File.exists?(INPUT_DATA_FILE)
        File.open(INPUT_DATA_FILE, 'w') { |f| f.puts generate_text(WORD_COUNT) }
      end
      File.read(INPUT_DATA_FILE)
    end

    def self.split_data(input_data)
      slice_text(input_data, THREADS).map { |el| el.split(Ruby3::MapReduce::DELIMITER) }
    end

    def self.mapping_data_plain_ruby(input_data)
      input_data.map do |el|
        el.uniq.reduce({}) do |a, e|
          a[e] = el.count(e)
          a
        end
      end
    end

    def self.mapping_data_green_threads(input_data)
      mu = Mutex.new
      res = []
      threads = []
      input_data.each do |el|
        threads << Thread.new do
          inner_data = \
            el.uniq.reduce({}) do |a, e|
              a[e] = el.count(e)
              a
            end
          mu.synchronize do
            res << inner_data
          end
        end
      end
      threads.map(&:join)
      res
    end

    def self.mapping_data_system_threads(input_data)
      processes = []
      pipes = []

      input_data.each do |el|
        pipe_out, pipe_in = IO.pipe
        started_pid = Process.fork do
          pipe_out.close # we aren't using the reader pipe inside the forked process
          inner_data = \
            el.uniq.reduce({}) do |a, e|
              a[e] = el.count(e)
              a
            end
          pipe_in.write(JSON.dump(inner_data))
        end
        pipe_in.close
        processes.push(started_pid)
        pipes.push(pipe_out)
      end
      
      result = []
      pipes.each do |pipe|
        out = pipe.read # Or read in a loop until it returns ""
        result << JSON.parse(out)
        pipe.close
      end
      
      # Wait for all child pids
      until processes.empty?
        dead_pid = Process.waitpid(0)
        processes -= [ dead_pid ]
      end

      result
    end

    def self.mapping_data_ractors(input_data)
      rs = input_data.map do |el|
        Ractor.new(el) do |el|
          el.uniq.reduce({}) do |a, e|
            a[e] = el.count(e)
            a
          end
        end
      end
      res = []
      until rs.empty?
        r, v = Ractor.select(*rs)
        rs.delete r
        res << v
      end
      res
    end

    def self.merge_data(input_data)
      input_data.reduce({}) do |a, e|
        e.each do |k, v|
          a[k] = (a[k] || 0) + v
        end
        a
      end
    end

    def self.reduce_data(input_data)
      (input_data.values.reduce(:+) / input_data.values.size.to_f).round(1)
    end
  end
end