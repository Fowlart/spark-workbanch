package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object ReadReservationDetailFromCsv extends App {

  val spark = SparkSession.builder()
    .appName("ReadReservationDetailFromCsv")
    .config("spark.master", "local")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  val rd = spark
    .read
    .option("header", "true")
    .option("schema", "`net_sales_usd_amount` DOUBLE COMMENT 'It is the total sales made within a specified time frame minus any sales returns, discounts, and sales allowances. USD currency means that for locations different from the USA exchange rate should be taken into account. ',`net_sales_local_amount` DOUBLE COMMENT 'It is the total sales made within a specified time frame minus any sales returns, discounts, and sales allowances.The amount specified in local currency(for ex. CAD) ',`ticket_id` STRING,`attendee_first_name` STRING,`attendee_last_name` STRING,`seasonal_type` STRING,`payment_auth_amount` DOUBLE,`activity_start_time` TIMESTAMP,`activity_end_time` TIMESTAMP,`dp_create_timestamp` STRING,`fee_charged` DOUBLE,`list_price` DOUBLE,`dp_update_timestamp` STRING,`reservation_id` STRING,`reservation_source` STRING,`order_number` STRING,`activity_name_internal` STRING,`duration_type` STRING,`paid_type` STRING,`is_walkin` STRING,`tip_added` STRING,`attendee_email` STRING,`check_in_timestamp` TIMESTAMP,`brand_id` STRING,`attendee_phone_number` STRING,`reservation_date` DATE,`no_show_fee_charged` STRING,`brand_world` STRING,`payment_capture_amount` DOUBLE,`store_number` STRING,`no_show_timestamp` TIMESTAMP,`bi_id` STRING,`reservation_date_timestamp` TIMESTAMP,`reservation_booked_timestamp` TIMESTAMP,`tip_amount` DOUBLE,`is_ghost` STRING,`sephora_id` BIGINT,`reservation_preferences` ARRAY<STRUCT<`reservationPrefId`: STRING, `reservationPrefName`: STRING, `reservationPrefValue`: STRING, `reservationPrefType`: STRING>>,`reservation_preference_id` STRING,`reservation_preference_name` STRING,`reservation_preference_type` STRING,`reservation_preference_value` STRING,`current_statusupdated_timestamp` TIMESTAMP,`activity_name_external` STRING,`brand_name` STRING,`is_no_show` STRING,`world_type` STRING,`program_type` STRING,`confirmation_number` STRING,`location_type` STRING,`is_checked_in` STRING,`created_timestamp` TIMESTAMP,`sku_number` STRING,`activity_duration` INT,`cancel_timestamp` TIMESTAMP,`activity_timezone` STRING,`current_status` STRING,`is_cancelled` STRING,`beauty_advisor_id` STRING,`is_ba_chosen_by_client` INT,`client_preferred_ba_id` STRING,`no_show_fee_amount` DOUBLE,`currency_code` STRING,`reservation_booked_date` DATE,`topic` STRING,`partition` INT,`offset` BIGINT,`timestamp` BIGINT,`transaction_date` STRING,`terminal_number` STRING,`sequence_number` STRING,`dp_order_number` STRING,`world` STRING")
    .csv("/Users/artur/Downloads/reservation_detail.csv")

  val customSchema = StructType.fromDDL("`net_sales_usd_amount` DOUBLE COMMENT 'It is the total sales made within a specified time frame minus any sales returns, discounts, and sales allowances. USD currency means that for locations different from the USA exchange rate should be taken into account. ',`net_sales_local_amount` DOUBLE COMMENT 'It is the total sales made within a specified time frame minus any sales returns, discounts, and sales allowances.The amount specified in local currency(for ex. CAD) ',`ticket_id` STRING,`attendee_first_name` STRING,`attendee_last_name` STRING,`seasonal_type` STRING,`payment_auth_amount` DOUBLE,`activity_start_time` TIMESTAMP,`activity_end_time` TIMESTAMP,`dp_create_timestamp` STRING,`fee_charged` DOUBLE,`list_price` DOUBLE,`dp_update_timestamp` STRING,`reservation_id` STRING,`reservation_source` STRING,`order_number` STRING,`activity_name_internal` STRING,`duration_type` STRING,`paid_type` STRING,`is_walkin` STRING,`tip_added` STRING,`attendee_email` STRING,`check_in_timestamp` TIMESTAMP,`brand_id` STRING,`attendee_phone_number` STRING,`reservation_date` DATE,`no_show_fee_charged` STRING,`brand_world` STRING,`payment_capture_amount` DOUBLE,`store_number` STRING,`no_show_timestamp` TIMESTAMP,`bi_id` STRING,`reservation_date_timestamp` TIMESTAMP,`reservation_booked_timestamp` TIMESTAMP,`tip_amount` DOUBLE,`is_ghost` STRING,`sephora_id` BIGINT,`reservation_preferences` ARRAY<STRUCT<`reservationPrefId`: STRING, `reservationPrefName`: STRING, `reservationPrefValue`: STRING, `reservationPrefType`: STRING>>,`reservation_preference_id` STRING,`reservation_preference_name` STRING,`reservation_preference_type` STRING,`reservation_preference_value` STRING,`current_statusupdated_timestamp` TIMESTAMP,`activity_name_external` STRING,`brand_name` STRING,`is_no_show` STRING,`world_type` STRING,`program_type` STRING,`confirmation_number` STRING,`location_type` STRING,`is_checked_in` STRING,`created_timestamp` TIMESTAMP,`sku_number` STRING,`activity_duration` INT,`cancel_timestamp` TIMESTAMP,`activity_timezone` STRING,`current_status` STRING,`is_cancelled` STRING,`beauty_advisor_id` STRING,`is_ba_chosen_by_client` INT,`client_preferred_ba_id` STRING,`no_show_fee_amount` DOUBLE,`currency_code` STRING,`reservation_booked_date` DATE,`topic` STRING,`partition` INT,`offset` BIGINT,`timestamp` BIGINT,`transaction_date` STRING,`terminal_number` STRING,`sequence_number` STRING,`dp_order_number` STRING,`world` STRING")

  val output = spark.createDataFrame(rd.rdd, customSchema)

  output.write.mode("overwrite")
    .format("delta")
    .save(s"src/main/resources/data/reservation_details")


}
