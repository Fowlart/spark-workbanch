package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, to_date}

  object ReservationDetailHDL extends App {

    val spark = SparkSession.builder()
      .appName("ReservationDetailDataCheck")
      .config("spark.master", "local")
      .config("spark.sql.codegen.wholeStage", "false")
      .getOrCreate()

    val updateData = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/artur/Documents/RESERVATION_DETAILS/PROVIDED_FILES")

    val reservationDetailFull = spark
      .read
      .format("delta")
      .load("/Users/artur/Documents/RESERVATION_DETAILS/RESERVATION_DETAILS_STAGE")

    val joined = reservationDetailFull.as("r").join(updateData.as("u"),
      col("r.confirmation_number") === col("u.confirmationId"), "left")
      .select(
        coalesce(to_date(col("startDateTime"),"yyyy-MM-dd HH:mm:ss"),col("reservation_date")).as("reservation_date"),
        coalesce(col("startDateTime"),col("reservation_date_timestamp")).as("reservation_date_timestamp"),
        coalesce(col("u.no_show_timestamp"),col("r.no_show_timestamp")).as("no_show_timestamp"),
        coalesce(col("updateTimestamp"),col("current_statusupdated_timestamp")).as("current_statusupdated_timestamp"),
        coalesce(col("createdTimeStamp"),col("created_timestamp")).as("created_timestamp"),
        coalesce(col("timeZone"),col("activity_timezone")).as("activity_timezone"),
        coalesce(to_date(col("booking_date"),"yyyy-MM-dd HH:mm:ss"),col("reservation_booked_date")).as("reservation_booked_date"),

        col("ticket_id"),

        col( "attendee_first_name"),

        col("attendee_last_name"),

        col("seasonal_type"),

        col("payment_auth_amount"),

        col("activity_start_time"),

        col("activity_end_time"),

        col("dp_create_timestamp"),

        col("fee_charged"),

        col("list_price"),

        col("dp_update_timestamp"),

        col("reservation_id"),

        col("reservation_source"),

        col("order_number"),

        col("activity_name_internal"),

        col("duration_type"),

        col("paid_type"),

        col("is_walkin"),

        col("tip_added"),

        col("attendee_email"),

        col("check_in_timestamp"),

        col("brand_id"),

        col("attendee_phone_number"),

        col("no_show_fee_charged"),

        col("brand_world"),

        col("payment_capture_amount"),

        col("store_number"),

        col("bi_id"),

        col("reservation_booked_timestamp"),

        col("tip_amount"),

        col("is_ghost"),

        col("sephora_id"),

        col("reservation_preferences"),

        col("reservation_preference_id"),

        col("reservation_preference_name"),

        col( "reservation_preference_type"),

        col("reservation_preference_value"),

        col("activity_name_external"),

        col("brand_name"),

        col("is_no_show"),

        col("world_type"),

        col("program_type"),

        col("confirmation_number"),

        col("location_type"),

        col("is_checked_in"),

        col("sku_number"),

        col("activity_duration"),

        col("cancel_timestamp"),

        col("current_status"),

        col("is_cancelled"),

        col("beauty_advisor_id"),

        col("is_ba_chosen_by_client"),

        col("client_preferred_ba_id"),

        col("no_show_fee_amount"),

        col("currency_code"),

        col("topic"),

        col("partition"),

        col("offset"),

        col("timestamp"),

        col("transaction_date"),

        col("terminal_number"),

        col("sequence_number"),

        col("dp_order_number"),

        col("world"))

      .filter(col("ticket_id").isin("KV7P3EOXEG", "9374QLVJZ0","LE70RVRXZG","MEXODLGJVQ","MG73N6Y76N")).show()
}
