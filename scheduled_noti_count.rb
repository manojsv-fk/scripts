def notification_rule_counter(rule_id)
  count = 0
  (0..96).each do |num|
    key = "Scheduled-notification-rules-bucket-#{num}"
    arr = REDIS.smembers(key)
    if arr.include?(rule_id.to_s)
      count = count + 1 
      puts "#{key} includes rule_id"
      # REDIS.srem(key, rule_id)
    end
  end
  count
end

notification_rule_counter(9289)

"Scheduled-notification-rules-bucket-68"

[52,4].each do |i|
  key = "Scheduled-notification-rules-bucket-#{i}"
  puts key
  REDIS.sadd(key, 9289)
end