class RedisOperations
  REDIS = Redis.new(host: ENV['REDIS_HOST'], port: ENV['REDIS_PORT'])
  def initialize(pattern, output_file)
    @cursor = 0
    @pattern = pattern
    @output_file = output_file
    @all_redis_keys = []
  end

  def store_keys_matching_pattern
    File.open(@output_file, "w+") do |f|
      while true
        s = REDIS.scan(@cursor, match: @pattern)
        f.puts(s.last)
        @all_redis_keys += s.last
        @cursor = s.first
        puts "cursor is at #{@cursor}"
        break if @cursor.to_i == 0
      end
    end
  end

  def get_redis_keys
    puts "Total of #{@all_redis_keys.count} keys"
    @all_redis_keys
  end

  def delete_redis_keys
    @all_redis_keys.each_slice(100).to_a.each_with_index do |batch, index|
      puts "Batch #{index} to delete"
      p batch
      REDIS.del(*batch)
      sleep 2
    end
  end

  def delete_keys_from_file(output_file)
    @all_redis_keys.clear
    file = File.open(output_file, "r")
    file.each do |data|
      @all_redis_keys << data.chomp
    end
    delete_redis_keys
  end
end

redis_operations = RedisOperations.new("manoj*", "/Users/manojsv/Documents/GitHub/manoj.txt")
redis_operations.store_keys_matching_pattern
redis_operations.get_redis_keys
redis_operations.delete_redis_keys

redis_operations.delete_keys_from_file("/Users/manojsv/Documents/GitHub/manoj.txt")