class LoadDeleter
  CARRIER_NAME_TO_SCAC_HASH = { 'Innocenti Depositi' => ['T3002563'] , 'TM Transportation' => ['T50474650'] ,
    'Konig' => ['T50518326', 'T1024177', 'T50474782'] , 'FrachtFWO' => ['T1105393'] , 
    'Eurogate' => ['T1091035', 'T30620'], 'Ewals Cargo' => ['T97929'], 'MDI' => ['T50287362'] }


  def initialize(trackings)
    @trackings = trackings
    @deleted_load_ids = []
  end

  def process_loads_to_be_deleted
    CARRIER_NAME_TO_SCAC_HASH.each do |carrier_name, original_scac|
      loads_belonging_to_company = @trackings.where(carrier_name: carrier_name, original_scac: original_scac)
      loads_belonging_to_company.each do |load|
        puts "Load to be deleted #{load.load_number} belonging to #{load.carrier_name} - #{load.original_scac}"
        delete_load(load)
      end
    end
  end

  def delete_load(tracking)
    tracking.update(deleted: true)
    tracking.update(deleted_by: 'manojsv')
    tracking.update(deleted_at: DateTime.now.utc)
    if tracking.truck_number.present?
      REDIS.del("ProcessTruckRecordTask::Load#{tracking.load_number.to_s.downcase.strip}::Truck#{tracking.truck_number.to_s.downcase.strip}")
      REDIS.del("ProcessTruckRecordTask::Load#{tracking.load_number.to_s.downcase.strip}::CurrentTruck")
    end
    @deleted_load_ids << tracking.id
  end

  def get_deleted_loads
    @deleted_load_ids
  end

end


trackings = Tracking.where(shipper_id: 'unilever')

load_deleter = LoadDeleter.new(trackings)
load_deleter.process_loads_to_be_deleted

load_deleter.get_deleted_loads


