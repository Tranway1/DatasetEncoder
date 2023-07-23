package edu.uchicago.cs.encsel.query.tpch

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

object TPCHSchema {

  val customerSchema = new MessageType("customer",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cust_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "address"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "phone"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "acct_bal"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "mkt_segment"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val nationSchema = new MessageType("nation",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "region_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val regionSchema = new MessageType("region",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "region_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val supplierSchema = new MessageType("supplier",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "address"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "phone"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "acct_bal"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val partSchema = new MessageType("part",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "mfgr"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "brand"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "type"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "size"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "container"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "retail_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val partsuppSchema = new MessageType("partsupp",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "avail_qty"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "supply_cost"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val lineitemSchema = new MessageType("lineitem",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "line_number"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "quantity"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "extended_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "discount"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "tax"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "return_flag"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "line_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "commit_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "receipt_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_instruct"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_mode"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )



  val customer_demographicsSchema = new MessageType("customer_demographics",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cd_demo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "cd_gender"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "cd_marital_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "cd_education_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cd_purchase_estimate"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "cd_credit_rating"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cd_dep_count"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cd_dep_employed_count"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cd_dep_college_count")
  )


  val catalog_salesSchema = new MessageType("catalog_sales",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_sold_date_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_sold_time_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_date_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_bill_customer_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_bill_cdemo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_bill_hdemo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_bill_addr_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_customer_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_cdemo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_hdemo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_addr_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_call_center_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_catalog_page_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_ship_mode_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_warehouse_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_item_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_promo_sk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "cs_order_number"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cs_quantity"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_wholesale_cost"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_list_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_sales_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_discount_amt"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_sales_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_wholesale_cost"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_list_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_tax"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_coupon_amt"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_ext_ship_cost"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_net_paid"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_net_paid_inc_tax"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_net_paid_inc_ship"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_net_paid_inc_ship_tax"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "cs_net_profit")
  )

  val lineitemOptSchema = new MessageType("lineitem",
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "line_number"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "quantity"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "extended_price"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "discount"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "tax"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "return_flag"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "line_status"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "commit_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "receipt_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_instruct"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_mode"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "comment")
  )

  val orderSchema = new MessageType("orders",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cust_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "total_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_priority"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "clerk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_priority"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val taxiSchema = new MessageType("taxi",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "medallion"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "hack_license"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "vendor_id"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "rate_code"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "store_and_fwd_flag"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "pickup_datetime"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "dropoff_datetime"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "passenger_count"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "trip_time_in_secs"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "trip_distance"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "pickup_longitude"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "pickup_latitude"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "dropoff_longitude"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "dropoff_latitude"),
  )

  val schemas = Array(customerSchema, nationSchema,
    regionSchema, supplierSchema, partSchema,
    partsuppSchema, lineitemSchema, orderSchema)
}
