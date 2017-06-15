/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonDataPerformance {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/presto/test/store"
    val warehouse = s"$rootPath/integration/presto/test/warehouse"
    val metastoredb = s"$rootPath/integration/presto/test"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS NATION")
    spark.sql("DROP TABLE IF EXISTS REGION")
    spark.sql("DROP TABLE IF EXISTS PART")
    spark.sql("DROP TABLE IF EXISTS PARTSUPP")
    spark.sql("DROP TABLE IF EXISTS SUPPLIER")
    spark.sql("DROP TABLE IF EXISTS CUSTOMER")
    spark.sql("DROP TABLE IF EXISTS ORDERS")
    spark.sql("DROP TABLE IF EXISTS LINEITEM")


    // Create table
    spark.sql("CREATE TABLE NATION  ( " +
              "N_NATIONKEY  INT," +
              "N_NAME STRING," +
              "N_REGIONKEY  INT," +
              "N_COMMENT STRING) " +
              "STORED BY 'carbondata'")

    spark.sql("CREATE TABLE REGION ( R_REGIONKEY INT ,\n R_NAME STRING ,\n R_COMMENT STRING) " +
              "STORED BY 'carbondata'")

    spark.sql("CREATE TABLE PART ( P_PARTKEY INT ,\n P_NAME STRING ,\n P_MFGR STRING ,\n P_BRAND " +
              "STRING ,\n P_TYPE STRING ,\n P_SIZE INT ,\n P_CONTAINER STRING ,\n P_RETAILPRICE " +
              "DECIMAL(15,2) ,\n P_COMMENT STRING ) STORED BY 'carbondata'")

    spark.sql("CREATE TABLE SUPPLIER ( S_SUPPKEY INT ,\n S_NAME STRING ,\n S_ADDRESS STRING ,\n " +
              "S_NATIONKEY INT ,\n S_PHONE STRING ,\n S_ACCTBAL DECIMAL(15,2) ,\n S_COMMENT " +
              "STRING ) STORED BY 'carbondata'")

    spark.sql("CREATE TABLE PARTSUPP ( PS_PARTKEY INT ,\n PS_SUPPKEY INT ,\n PS_AVAILQTY INT ,\n " +
              "PS_SUPPLYCOST DECIMAL(15,2) ,\n PS_COMMENT STRING ) STORED BY 'carbondata'")

    spark.sql("CREATE TABLE CUSTOMER ( C_CUSTKEY INT ,\n C_NAME STRING ,\n C_ADDRESS STRING ,\n " +
              "C_NATIONKEY INT ,\n C_PHONE STRING ,\n C_ACCTBAL DECIMAL(15,2) ,\n C_MKTSEGMENT " +
              "STRING ,\n C_COMMENT STRING ) STORED BY 'carbondata'")

    spark.sql("CREATE TABLE ORDERS ( O_ORDERKEY INT ,\n O_CUSTKEY INT ,\n O_ORDERSTATUS STRING ," +
              "\n O_TOTALPRICE DECIMAL(15,2) ,\n O_ORDERDATE TIMESTAMP ,\n O_ORDERPRIORITY STRING" +
              " , \n O_CLERK STRING , \n O_SHIPPRIORITY INT ,\n O_COMMENT STRING ) STORED BY " +
              "'carbondata'")

    spark.sql("CREATE TABLE LINEITEM ( L_ORDERKEY INT ,\n L_PARTKEY INT ,\n L_SUPPKEY INT ,\n " +
              "L_LINENUMBER INT ,\n L_QUANTITY DECIMAL(15,2) ,\n L_EXTENDEDPRICE DECIMAL(15,2) ," +
              "\n L_DISCOUNT DECIMAL(15,2) ,\n L_TAX DECIMAL(15,2) ,\n L_RETURNFLAG STRING ,\n " +
              "L_LINESTATUS STRING ,\n L_SHIPDATE TIMESTAMP ,\n L_COMMITDATE TIMESTAMP ,\n " +
              "L_RECEIPTDATE TIMESTAMP ,\n L_SHIPINSTRUCT STRING ,\n L_SHIPMODE STRING ,\n " +
              "L_COMMENT STRING ) STORED BY 'carbondata'")

    spark.sql("LOAD DATA INPATH 'hdfs://localhost:54310/user/nation.csv' INTO TABLE nation " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='N_NATIONKEY,N_NAME," +
              "N_REGIONKEY,N_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/region.csv\" INTO TABLE region " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='R_REGIONKEY,R_NAME," +
              "R_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/part.csv\" INTO TABLE part OPTIONS" +
              "('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND," +
              "P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/supplier.csv\" INTO TABLE supplier " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' S_SUPPKEY,             " +
              "S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/partsupp.csv\" INTO TABLE partsupp " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY ," +
              "PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/customer.csv\" INTO TABLE customer " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"' , 'FILEHEADER'='C_CUSTKEY,C_NAME," +
              "C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/orders.csv\" INTO TABLE orders " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'='O_ORDERKEY,O_CUSTKEY," +
              "O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY," +
              "O_COMMENT')")

    spark.sql("LOAD DATA INPATH \"hdfs://localhost:54310/user/lineitem.csv\" INTO TABLE lineitem " +
              "OPTIONS('DELIMITER'='|' , 'QUOTECHAR'='\"','FILEHEADER'=' L_ORDERKEY,L_PARTKEY," +
              "L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG," +
              "L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE," +
              "L_COMMENT')")

    spark.sql("SELECT * FROM NATION").show()
    spark.sql("SELECT * FROM REGION").show()
    spark.sql("SELECT * FROM PART").show()
    spark.sql("SELECT * FROM SUPPLIER").show()
    spark.sql("SELECT * FROM PARTSUPP").show()
    spark.sql("SELECT * FROM CUSTOMER").show()
    spark.sql("SELECT * FROM ORDERS").show()
    spark.sql("SELECT * FROM LINEITEM").show()


    try {
//      spark.sql("drop view if exists q11_part_tmp_cached").show()
     /* spark.sql("drop view if exists q11_sum_tmp_cached").show()*/
   /*   spark.sql("drop view if exists revenue_cached").show()*/
  /*    spark.sql("drop view if exists max_revenue_cached").show()*/
/*      spark.sql("drop view if exists q18_tmp_cached").show()*/
      spark.sql("drop table if exists q18_large_volume_customer_cached").show()
 /*     spark.sql("drop view if exists q22_customer_tmp_cached").show()*/
     /* spark.sql("drop view if exists q22_customer_tmp1_cached").show()*/
  /*    spark.sql("drop view if exists q22_orders_tmp_cached").show()*/

          spark.sql("select\n        l_returnflag,\n        l_linestatus,\n        sum(l_quantity) as " +
              "sum_qty,\n        sum(l_extendedprice) as sum_base_price,\n        sum" +
              "(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n        sum" +
              "(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n        avg" +
              "(l_quantity) as avg_qty,\n        avg(l_extendedprice) as avg_price,\n        avg" +
              "(l_discount) as avg_disc,\n        count(*) as count_order\nfrom\n        " +
              "lineitem\nwhere\n        l_shipdate <= '1998-09-16'\ngroup by\n        " +
              "l_returnflag,\n        l_linestatus\norder by\n        l_returnflag,\n        " +
              "l_linestatus").show()

          println("0")

      /*spark.sql("create view q2_min_ps_supplycost as\nselect\n        p_partkey as min_p_partkey," +
                "\n        min(ps_supplycost) as min_ps_supplycost\nfrom\n        part,\n        " +
                "partsupp,\n        supplier,\n        nation,\n        region\nwhere\n        " +
                "p_partkey = ps_partkey\n        and s_suppkey = ps_suppkey\n        and " +
                "s_nationkey = n_nationkey\n        and n_regionkey = r_regionkey\n        and " +
                "r_name = 'EUROPE'\ngroup by\n        p_partkey").show()*/

      println("1")

      /*spark.sql("select\n        s_acctbal,\n        s_name,\n        n_name,\n        p_partkey," +
                "\n        p_mfgr,\n        s_address,\n        s_phone,\n        " +
                "s_comment\nfrom\n        part,\n        supplier,\n        partsupp,\n        " +
                "nation,\n        region,\n        q2_min_ps_supplycost\nwhere\n        p_partkey" +
                " = ps_partkey\n        and s_suppkey = ps_suppkey\n        and p_size = 37\n    " +
                "    and p_type like '%COPPER'\n        and s_nationkey = n_nationkey\n        " +
                "and n_regionkey = r_regionkey\n        and r_name = 'EUROPE'\n        and " +
                "ps_supplycost = min_ps_supplycost\n        and p_partkey = min_p_partkey\norder " +
                "by\n        s_acctbal desc,\n        n_name,\n        s_name,\n        " +
                "p_partkey\nlimit 100").show()*/

      println("2")

      spark.sql("select\n        l_orderkey,\n        sum(l_extendedprice * (1 - l_discount)) as " +
                "revenue,\n        o_orderdate,\n        o_shippriority\nfrom\n        customer," +
                "\n        orders,\n        lineitem\nwhere\n        c_mktsegment = 'BUILDING'\n " +
                "       and c_custkey = o_custkey\n        and l_orderkey = o_orderkey\n        " +
                "and o_orderdate < '1995-03-22'\n        and l_shipdate > '1995-03-22'\ngroup " +
                "by\n        l_orderkey,\n        o_orderdate,\n        o_shippriority\norder " +
                "by\n        revenue desc,\n        o_orderdate\nlimit 10").show()
      println("3")

      spark.sql("select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n       " +
                " orders as o\nwhere\n        o_orderdate >= '1996-05-01'\n        and " +
                "o_orderdate < '1996-08-01'\n        and exists (\n                select\n      " +
                "                  *\n                from\n                        lineitem\n   " +
                "             where\n                        l_orderkey = o.o_orderkey\n         " +
                "               and l_commitdate < l_receiptdate\n        )\ngroup by\n        " +
                "o_orderpriority\norder by\n        o_orderpriority").show()

      println("4")
      spark.sql("select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
                "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
                "supplier,\n        nation,\n        region\nwhere\n        c_custkey = " +
                "o_custkey\n        and l_orderkey = o_orderkey\n        and l_suppkey = " +
                "s_suppkey\n        and c_nationkey = s_nationkey\n        and s_nationkey = " +
                "n_nationkey\n        and n_regionkey = r_regionkey\n        and r_name = " +
                "'AFRICA'\n        and o_orderdate >= '1993-01-01'\n        and o_orderdate < " +
                "'1994-01-01'\ngroup by\n        n_name\norder by\n        revenue desc").show()

      println("5")
      spark.sql("select\n        sum(l_extendedprice * l_discount) as revenue\nfrom\n        " +
                "lineitem\nwhere\n        l_shipdate >= '1993-01-01'\n        and l_shipdate < " +
                "'1994-01-01'\n        and l_discount between 0.06 - 0.01 and 0.06 + 0.01\n      " +
                "  and l_quantity < 25").show()

      println("6")
      spark.sql("select\n        supp_nation,\n        cust_nation,\n        l_year,\n        sum" +
                "(volume) as revenue\nfrom\n        (\n                select\n                  " +
                "      n1.n_name as supp_nation,\n                        n2.n_name as " +
                "cust_nation,\n                        year(l_shipdate) as l_year,\n             " +
                "           l_extendedprice * (1 - l_discount) as volume\n                from\n " +
                "                       supplier,\n                        lineitem,\n           " +
                "             orders,\n                        customer,\n                       " +
                " nation n1,\n                        nation n2\n                where\n         " +
                "               s_suppkey = l_suppkey\n                        and o_orderkey = " +
                "l_orderkey\n                        and c_custkey = o_custkey\n                 " +
                "       and s_nationkey = n1.n_nationkey\n                        and c_nationkey" +
                " = n2.n_nationkey\n                        and (\n                              " +
                "  (n1.n_name = 'KENYA' and n2.n_name = 'PERU')\n                                " +
                "or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')\n                        )\n    " +
                "                    and l_shipdate between '1995-01-01' and '1996-12-31'\n      " +
                "  ) as shipping\ngroup by\n        supp_nation,\n        cust_nation,\n        " +
                "l_year\norder by\n        supp_nation,\n        cust_nation,\n        l_year").show()

      println("7")
      spark.sql("select\n        o_year,\n        sum(case\n                when nation = 'PERU' " +
                "then volume\n                else 0\n        end) / sum(volume) as " +
                "mkt_share\nfrom\n        (\n                select\n                        year" +
                "(o_orderdate) as o_year,\n                        l_extendedprice * (1 - " +
                "l_discount) as volume,\n                        n2.n_name as nation\n           " +
                "     from\n                        part,\n                        supplier,\n   " +
                "                     lineitem,\n                        orders,\n               " +
                "         customer,\n                        nation n1,\n                        " +
                "nation n2,\n                        region\n                where\n             " +
                "           p_partkey = l_partkey\n                        and s_suppkey = " +
                "l_suppkey\n                        and l_orderkey = o_orderkey\n                " +
                "        and o_custkey = c_custkey\n                        and c_nationkey = n1" +
                ".n_nationkey\n                        and n1.n_regionkey = r_regionkey\n        " +
                "                and r_name = 'AMERICA'\n                        and s_nationkey " +
                "= n2.n_nationkey\n                        and o_orderdate between '1995-01-01' " +
                "and '1996-12-31'\n                        and p_type = 'ECONOMY BURNISHED " +
                "NICKEL'\n        ) as all_nations\ngroup by\n        o_year\norder by\n        " +
                "o_year").show()

      println("8")
      spark.sql("select\n        nation,\n        o_year,\n        sum(amount) as " +
                "sum_profit\nfrom\n        (\n                select\n                        " +
                "n_name as nation,\n                        year(o_orderdate) as o_year,\n       " +
                "                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity" +
                " as amount\n                from\n                        part,\n               " +
                "         supplier,\n                        lineitem,\n                        " +
                "partsupp,\n                        orders,\n                        nation\n    " +
                "            where\n                        s_suppkey = l_suppkey\n              " +
                "          and ps_suppkey = l_suppkey\n                        and ps_partkey = " +
                "l_partkey\n                        and p_partkey = l_partkey\n                  " +
                "      and o_orderkey = l_orderkey\n                        and s_nationkey = " +
                "n_nationkey\n                        and p_name like '%plum%'\n        ) as " +
                "profit\ngroup by\n        nation,\n        o_year\norder by\n        nation,\n  " +
                "      o_year desc").show()

      println("9")
      spark.sql("select\n        c_custkey,\n        c_name,\n        sum(l_extendedprice * (1 - " +
                "l_discount)) as revenue,\n        c_acctbal,\n        n_name,\n        " +
                "c_address,\n        c_phone,\n        c_comment\nfrom\n        customer,\n      " +
                "  orders,\n        lineitem,\n        nation\nwhere\n        c_custkey = " +
                "o_custkey\n        and l_orderkey = o_orderkey\n        and o_orderdate >= " +
                "'1993-07-01'\n        and o_orderdate < '1993-10-01'\n        and l_returnflag =" +
                " 'R'\n        and c_nationkey = n_nationkey\ngroup by\n        c_custkey,\n     " +
                "   c_name,\n        c_acctbal,\n        c_phone,\n        n_name,\n        " +
                "c_address,\n        c_comment\norder by\n        revenue desc\nlimit 20").show()

      println("10")
    /*  spark.sql("create view q11_part_tmp_cached as select\n        ps_partkey,\n        sum(ps_supplycost * ps_availqty) as " +
                "part_value\nfrom\n        partsupp,\n        supplier,\n        nation\nwhere\n " +
                "       ps_suppkey = s_suppkey\n        and s_nationkey = n_nationkey\n        " +
                "and n_name = 'GERMANY'\ngroup by ps_partkey").show()*/

   /*   spark.sql("select * from q11_part_tmp_cached").show()*/
          println("10.1")

     /* spark.sql("create view q11_sum_tmp_cached " +
                "as\nselect\n        sum(part_value) as total_value\nfrom\n        " +
                "q11_part_tmp_cached").show()*/

          println("10.2")
     /* spark.sql("\n\nselect\n        ps_partkey, part_value as value\nfrom " +
                "(\n        select\n                ps_partkey,\n                part_value,\n   " +
                "             total_value\n        from\n                q11_part_tmp_cached join" +
                " q11_sum_tmp_cached\n) a\nwhere\n        part_value > total_value * 0" +
                ".0001\norder by\n        value desc").show()
*/
      println("11")
      spark.sql("select\n        l_shipmode,\n        sum(case\n                when " +
                "o_orderpriority = '1-URGENT'\n                        or o_orderpriority = " +
                "'2-HIGH'\n                        then 1\n                else 0\n        end) " +
                "as high_line_count,\n        sum(case\n                when o_orderpriority <> " +
                "'1-URGENT'\n                        and o_orderpriority <> '2-HIGH'\n           " +
                "             then 1\n                else 0\n        end) as " +
                "low_line_count\nfrom\n        orders,\n        lineitem\nwhere\n        " +
                "o_orderkey = l_orderkey\n        and l_shipmode in ('REG AIR', 'MAIL')\n        " +
                "and l_commitdate < l_receiptdate\n        and l_shipdate < l_commitdate\n       " +
                " and l_receiptdate >= '1995-01-01'\n        and l_receiptdate < " +
                "'1996-01-01'\ngroup by\n        l_shipmode\norder by\n        l_shipmode").show()

      println("12")
      spark.sql("select\n        c_count,\n        count(*) as custdist\nfrom\n        (\n       " +
                "         select\n                        c_custkey,\n                        " +
                "count(o_orderkey) as c_count\n                from\n                        " +
                "customer left outer join orders on\n                                c_custkey = " +
                "o_custkey\n                                and o_comment not like " +
                "'%unusual%accounts%'\n                group by\n                        " +
                "c_custkey\n        ) c_orders\ngroup by\n        c_count\norder by\n        " +
                "custdist desc,\n        c_count desc").show()

      println("13")
      spark.sql("select\n        100.00 * sum(case\n                when p_type like 'PROMO%'\n  " +
                "                      then l_extendedprice * (1 - l_discount)\n                " +
                "else 0\n        end) / sum(l_extendedprice * (1 - l_discount)) as " +
                "promo_revenue\nfrom\n        lineitem,\n        part\nwhere\n        l_partkey =" +
                " p_partkey\n        and l_shipdate >= '1995-08-01'\n        and l_shipdate < " +
                "'1995-09-01'").show()

      println("14")
    /*  spark.sql("create view revenue_cached as\nselect\n        l_suppkey as supplier_no,\n      " +
                "  sum(l_extendedprice * (1 - l_discount)) as total_revenue\nfrom\n        " +
                "lineitem\nwhere\n        l_shipdate >= '1996-01-01'\n        and l_shipdate < " +
                "'1996-04-01'\ngroup by l_suppkey").show()

          println("14.1")
       spark.sql("create view max_revenue_cached as\nselect\n" +
                "        max(total_revenue) as max_revenue\nfrom\n        revenue_cached").show()
          println("14.2")
       spark.sql("select\n        s_suppkey,\n        s_name,\n        s_address,\n        " +
                "s_phone,\n        total_revenue\nfrom\n        supplier,\n        " +
                "revenue_cached,\n        max_revenue_cached\nwhere\n        s_suppkey = " +
                "supplier_no\n        and total_revenue = max_revenue \norder by s_suppkey").show()*/

      println("15")
      spark.sql("select\n        p_brand,\n        p_type,\n        p_size,\n        count" +
                "(distinct ps_suppkey) as supplier_cnt\nfrom\n        partsupp,\n        " +
                "part\nwhere\n        p_partkey = ps_partkey\n        and p_brand <> 'Brand#34'\n" +
                "        and p_type not like 'ECONOMY BRUSHED%'\n        and p_size in (22, 14, " +
                "27, 49, 21, 33, 35, 28)\n        and partsupp.ps_suppkey not in (\n             " +
                "   select\n                        s_suppkey\n                from\n            " +
                "            supplier\n                where\n                        s_comment " +
                "like '%Customer%Complaints%'\n        )\ngroup by\n        p_brand,\n        " +
                "p_type,\n        p_size\norder by\n        supplier_cnt desc,\n        p_brand," +
                "\n        p_type,\n        p_size").show()

      println("16")
      spark.sql("with q17_part as (\n  select p_partkey from part where  \n  p_brand = " +
                "'Brand#23'\n  and p_container = 'MED BOX'\n),\nq17_avg as (\n  select l_partkey " +
                "as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity\n  from lineitem \n  where" +
                " l_partkey IN (select p_partkey from q17_part)\n  group by l_partkey\n)," +
                "\nq17_price as (\n  select\n  l_quantity,\n  l_partkey,\n  l_extendedprice\n  " +
                "from\n  lineitem\n  where\n  l_partkey IN (select p_partkey from q17_part)\n)" +
                "\nselect cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly\nfrom " +
                "q17_avg, q17_price\nwhere \nt_partkey = l_partkey and l_quantity < t_avg_quantity").show()

      println("17")
    /*  spark.sql("create view q18_tmp_cached as\nselect\n        l_orderkey,\n        sum" +
                "(l_quantity) as t_sum_quantity\nfrom\n        lineitem\nwhere\n        " +
                "l_orderkey is not null\ngroup by\n        l_orderkey").show()*/

      println("18")
    /*  spark.sql("create table q18_large_volume_customer_cached as\nselect\n        c_name,\n     " +
                "   c_custkey,\n        o_orderkey,\n        o_orderdate,\n        o_totalprice," +
                "\n        sum(l_quantity)\nfrom\n        customer,\n        orders,\n        " +
                "q18_tmp_cached t,\n        lineitem l\nwhere\n        c_custkey = o_custkey\n   " +
                "     and o_orderkey = t.l_orderkey\n        and o_orderkey is not null\n        " +
                "and t.t_sum_quantity > 300\n        and o_orderkey = l.l_orderkey\n        and l" +
                ".l_orderkey is not null\ngroup by\n        c_name,\n        c_custkey,\n        " +
                "o_orderkey,\n        o_orderdate,\n        o_totalprice\norder by\n        " +
                "o_totalprice desc,\n        o_orderdate \nlimit 100").show()*/

      println("19")
      spark.sql("select\n        sum(l_extendedprice* (1 - l_discount)) as revenue\nfrom\n       " +
                " lineitem,\n        part\nwhere\n        (\n                p_partkey = " +
                "l_partkey\n                and p_brand = 'Brand#32'\n                and " +
                "p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n                and " +
                "l_quantity >= 7 and l_quantity <= 7 + 10\n                and p_size between 1 " +
                "and 5\n                and l_shipmode in ('AIR', 'AIR REG')\n                and" +
                " l_shipinstruct = 'DELIVER IN PERSON'\n        )\n        or\n        (\n       " +
                "         p_partkey = l_partkey\n                and p_brand = 'Brand#35'\n      " +
                "          and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n    " +
                "            and l_quantity >= 15 and l_quantity <= 15 + 10\n                and " +
                "p_size between 1 and 10\n                and l_shipmode in ('AIR', 'AIR REG')\n " +
                "               and l_shipinstruct = 'DELIVER IN PERSON'\n        )\n        or\n" +
                "        (\n                p_partkey = l_partkey\n                and p_brand = " +
                "'Brand#24'\n                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', " +
                "'LG PKG')\n                and l_quantity >= 26 and l_quantity <= 26 + 10\n     " +
                "           and p_size between 1 and 15\n                and l_shipmode in " +
                "('AIR', 'AIR REG')\n                and l_shipinstruct = 'DELIVER IN PERSON'\n  " +
                "      )").show()
      println("20")
      spark.sql("-- explain formatted \n\nwith tmp1 as (\n    select p_partkey from part where p_name like 'forest%'\n)," +
                "\ntmp2 as (\n    select s_name, s_address, s_suppkey\n    from supplier, " +
                "nation\n    where s_nationkey = n_nationkey\n    and n_name = 'CANADA'\n),\ntmp3" +
                " as (\n    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey\n " +
                "   from lineitem, tmp2\n    where l_shipdate >= '1994-01-01' and l_shipdate <= " +
                "'1995-01-01'\n    and l_suppkey = s_suppkey \n    group by l_partkey, " +
                "l_suppkey\n),\ntmp4 as (\n    select ps_partkey, ps_suppkey, ps_availqty\n    " +
                "from partsupp \n    where ps_partkey IN (select p_partkey from tmp1)\n),\ntmp5 " +
                "as (\nselect\n    ps_suppkey\nfrom\n    tmp4, tmp3\nwhere\n    ps_partkey = " +
                "l_partkey\n    and ps_suppkey = l_suppkey\n    and ps_availqty > sum_quantity\n)" +
                "\nselect\n    s_name,\n    s_address\nfrom\n    supplier\nwhere\n    s_suppkey " +
                "IN (select ps_suppkey from tmp5)\norder by s_name").show()

      println("21")
      /*
      CREATE TEMPORARY TABLE is not supported yet. Please use CREATE TEMPORARY VIEW as an alternative.(line 3, pos 0)
       */
     /* spark.sql("-- explain\n\ncreate temporary table l3 stored as orc as \nselect l_orderkey, " +
                "count(distinct l_suppkey) as cntSupp\nfrom lineitem\nwhere l_receiptdate > " +
                "l_commitdate and l_orderkey is not null\ngroup by l_orderkey\nhaving cntSupp = 1").show()

      println("22")
      spark.sql("with location as (\nselect supplier.* from supplier, nation where\ns_nationkey =" +
                " n_nationkey and n_name = 'SAUDI ARABIA'\n)\nselect s_name, count(*) as " +
                "numwait\nfrom\n(\nselect li.l_suppkey, li.l_orderkey\nfrom lineitem li join " +
                "orders o on li.l_orderkey = o.o_orderkey and\n                      o" +
                ".o_orderstatus = 'F'\n     join\n     (\n     select l_orderkey, count(distinct " +
                "l_suppkey) as cntSupp\n     from lineitem\n     group by l_orderkey\n     ) l2 " +
                "on li.l_orderkey = l2.l_orderkey and \n             li.l_receiptdate > li" +
                ".l_commitdate and \n             l2.cntSupp > 1\n) l1 join l3 on l1.l_orderkey =" +
                " l3.l_orderkey\n join location s on l1.l_suppkey = s.s_suppkey\ngroup by\n " +
                "s_name\norder by\n numwait desc,\n s_name\nlimit 100").show()*/

      println("23")
    /*  spark.sql("create view if not exists q22_customer_tmp_cached as\nselect\n        c_acctbal," +
                "\n        c_custkey,\n        substr(c_phone, 1, 2) as cntrycode\nfrom\n        " +
                "customer\nwhere\n        substr(c_phone, 1, 2) = '13' or\n        substr" +
                "(c_phone, 1, 2) = '31' or\n        substr(c_phone, 1, 2) = '23' or\n        " +
                "substr(c_phone, 1, 2) = '29' or\n        substr(c_phone, 1, 2) = '30' or\n      " +
                "  substr(c_phone, 1, 2) = '18' or\n        substr(c_phone, 1, 2) = '17'").show()*/

      println("24")
    /*  spark.sql("create view if not exists q22_customer_tmp1_cached as\nselect\n        avg" +
                "(c_acctbal) as avg_acctbal\nfrom\n        q22_customer_tmp_cached\nwhere\n      " +
                "  c_acctbal > 0.00").show()*/

      println("25")
     /* spark.sql("create view if not exists q22_orders_tmp_cached as\nselect\n        " +
                "o_custkey\nfrom\n        orders\ngroup by\n        o_custkey").show()*/

      println("26")
     /* spark.sql("select\n        cntrycode,\n        count(1) as numcust,\n        sum(c_acctbal)" +
                " as totacctbal\nfrom (\n        select\n                cntrycode,\n            " +
                "    c_acctbal,\n                avg_acctbal\n        from\n                " +
                "q22_customer_tmp1_cached ct1 join (\n                        select\n           " +
                "                     cntrycode,\n                                c_acctbal\n    " +
                "                    from\n                                q22_orders_tmp_cached " +
                "ot\n                                right outer join q22_customer_tmp_cached " +
                "ct\n                                on ct.c_custkey = ot.o_custkey\n            " +
                "            where\n                                o_custkey is null\n          " +
                "      ) ct2\n) a\nwhere\n        c_acctbal > avg_acctbal\ngroup by\n        " +
                "cntrycode\norder by\n        cntrycode").show()*/

    }catch {
      case ex :Exception=>println(ex.getMessage)
    }

    spark.stop()

    System.exit(0)
  }

}
