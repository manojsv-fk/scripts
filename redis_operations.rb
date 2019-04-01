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
        s = REDIS.scan(@cursor, match: @pattern, count: 1000)
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
    @all_redis_keys.each_slice(1000).to_a.each_with_index do |batch, index|
      puts "Batch #{index} to delete"
      REDIS.del(*batch)
      puts "Deleted keys"
      sleep 0.1
    end
    @all_redis_keys.clear
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

redis_operations = RedisOperations.new("Google::ResolveTimezone::V2*", "/home/ec2-user/redis-keys2.txt")
start_time = Time.now
redis_operations.store_keys_matching_pattern
end_time = Time.now

puts "Time taken #{end_time - start_time}"
redis_operations.get_redis_keys
redis_operations.delete_redis_keys

redis_operations.delete_keys_from_file("/Users/manojsv/Documents/GitHub/manoj.txt")
redis_operations.delete_keys_from_file("/home/ec2-user/redis-keys2.txt")


