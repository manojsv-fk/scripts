class SuperAdminList
  def initialize(output_path)
    @super_admins = User.where(super_admin: true)
    @output_path = output_path
  end

  def create_list
    create_output_file
    @super_admins.each_with_index do |user, index|
      puts "Processing user no #{index} with email #{user.email_address}"
      arr = []
      user_name = [user.first_name.to_s, user.last_name.to_s].join(" ")
      email = user.email_address
      active = user.active.present?
      arr = [user_name, email, active]
      append_user(arr)
    end
  end

  def create_output_file
    CSV.open(@output_path, "wb") do |csv|
      csv << ["User Name", "Email", "Active"]
    end
  end

  def append_user(user_array)
    CSV.open(@output_path, "ab") do |csv|
      csv << user_array
    end
  end
end

# super_admins_list =  SuperAdminList.new("/Users/manojsv/Desktop/super_admins_local.csv")
super_admins_list =  SuperAdminList.new("/home/ec2-user/super_admins_uat.csv")
super_admins_list.create_list
