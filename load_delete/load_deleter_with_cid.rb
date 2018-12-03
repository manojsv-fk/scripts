class LoadDeleter
  CARRIER_ID_TO_SCAC_HASH = { 'innocenti-depositi' => ['T3002563'] , 'tm-transportation' => ['T50474650'] ,
    'konig' => ['T50518326', 'T1024177', 'T50474782'] , 'frachtfwo' => ['T1105393'] , 
    'eurogate' => ['T1091035', 'T30620'], 'ewals-intermodal' => ['T97929'], 'mdi' => ['T50287362'] }


  def initialize(trackings)
    @trackings = trackings
    @deleted_load_ids = []
  end

  def process_loads_to_be_deleted
    CARRIER_ID_TO_SCAC_HASH.each do |carrier_id, original_scac|
      loads_belonging_to_company = @trackings.where(carrier_id: carrier_id, original_scac: original_scac)
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


trackings = Tracking.where(shipper_id: 'unilever').not_deleted

load_deleter = LoadDeleter.new(trackings)
load_deleter.process_loads_to_be_deleted

load_deleter.get_deleted_loads


