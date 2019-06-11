class BackFillTags
  def initialize(tracking_ids)
    @tracking_ids = tracking_ids
  end

  def perform_backfill
    @tracking_ids.each_with_index do |t_id, index|
      backfill_load(t_id, index)
      if index % 4 == 0
        puts "sleeping"
        sleep 0.1
      end
    end
  end

  def backfill_load(t_id, index)
    begin
      puts "Index is at #{index}"
      load = FourKitesClients::TrackingClient.get_load(t_id)
      address_ids  = get_address_ids(load)
      puts "Address ids are #{address_ids}"
      tags = get_address_tags(address_ids)
      update_load(t_id, tags)
    rescue Exception => e
      puts "Exception occured #{exception.backtrace[0..5].join('\n')}"
      puts e.message 
    end
  end

  def get_address_ids(load)
    load.present? && load[:stops].present? && load[:stops].map{|s| s[:addressId]}.compact.uniq
  end

  def get_address_tags(address_ids)
    tags = []
    return tags if address_ids.blank?
    addresses = FourKitesClients::GeoClient.get_addresses(address_ids, '', [], {:not_use_statistics => 'yes'})
    addresses[:addresses].each do |addr|
      puts "Address"
      p addr
      tags += addr[:tags].to_a
    end
    tags
  end

  def update_load(t_id, tags)
    return if tags.blank?
    params = get_update_params(tags)
    FourKitesClients::TrackingClient.update_load(t_id, params)
    puts "Tags #{tags} updated for load #{t_id}"
  end

  def get_update_params(tags)
    load_params = {}
    load_params[:tags] = tags if tags.is_a?(Array)
    {load: load_params}
  end
end

tracking_ids = []
back_fill_tags = BackFillTags.new(tracking_ids)
back_fill_tags.perform_backfill




