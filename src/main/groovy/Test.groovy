import spark.api.java.*;
import spark.api.java.function.*;
import static repl.Repl.*;

import com.xlson.groovycsv.CsvParser;
import scala.Tuple2;
import org.codehaus.groovy.runtime.MethodClosure;
import org.joda.time.LocalDate;

import repl.F;
import repl.PairF;


class SimpleExpando  implements Serializable {
	public SimpleExpando(props){
		this.props = props
	}
	private static final long serialVersionUID = 1;

	def props = [:]
	def propertyMissing(String name ){
		return this.props[name]
	}

	def methodMissing(String name , args){
		return this.props[name](*args)
	}
}


class Test{

	public static PairF createCostArchiveKeyFunction(cost_archive_header){
		return new PairF({ s ->
			def row = CsvParser.parseCsv([separator:'\t' ,readFirstLine:true, columnNames: cost_archive_header], s )[0]
			def values = row.getValues()
			def key = row.cost_archive_server_campaign_fk + "_" + row.cost_archive_server_ad_group_fk + "_" + row.cost_archive_server_keyword_fk //+ "_" + row.cost_archive_date
			return new Tuple2(key, Arrays.asList(values))
		})
	}

	public static PairF createEventArchiveKeyFunction(event_archive_header){
		return new PairF({ s ->
			def row = CsvParser.parseCsv([readFirstLine:true, columnNames: event_archive_header], s )[0]
			def values = row.getValues()
			def key = row.event_archive_server_campaign_fk + "_" + row.event_archive_server_ad_group_fk + "_" + row.event_archive_server_keyword_fk  //+ "_" + row.event_archive_date
			return new Tuple2(key, Arrays.asList(values))
		 })
	}

	public static F createFilterForEventTypeFunction(event_archive_header, eventTypeSpecificityMap){
		return new F(
				{ tuple ->
					def key = tuple._1
					def row = tuple._2
					def rowMap = rowToEventColumnsMap(row, event_archive_header)
					return eventTypeSpecificityMap.every{ field, value ->
						"'" + value + "'" == rowMap[field.replaceFirst("event_lookup_", "")]
					}
				}
		)
	}

	public static def rowToEventColumnsMap(row, event_archive_header){ 
		def newRow = [:]
		event_archive_header.eachWithIndex{colName, i -> 
			newRow[colName.replaceFirst("event_archive_", "")] = row[i] 
		}
		return newRow
	}

	public static def rowToCostColumnsMap(row, cost_archive_header){
		def newRow = [:]
		cost_archive_header.eachWithIndex{colName, i -> 
			newRow[colName.replaceFirst("cost_archive_", "")] = row[i] 
		}
		return newRow
	}

	public static def getEventTypeSpecificityFields(){
		def sqlInstance = groovy.sql.Sql.newInstance("jdbc:jtds:sqlserver://10.10.59.61:8000;DatabaseName=example", 'optimine_manager', 'dummy', 'net.sourceforge.jtds.jdbc.Driver')
		def r = sqlInstance.rows("select * from event_lookup where event_lookup_group_id = 1171773;")
		//of these fields find the ones that aren't null for getting event data
		def eventTypeSpecificityFields = r[1].subMap(
			["event_lookup_ad_server_name", 
			 "event_lookup_account_name", 
			 "event_lookup_campaign_name", 
			 "event_lookup_ad_group_name", 
			 "event_lookup_match_type_name",
			 "event_lookup_event_name"]
		).inject([]){list, k, v -> 
			if(v != null){
				list << k
			}; 
			return list
		}
		
		return r[1].subMap(eventTypeSpecificityFields)
	}
	public static void setupGroovyRepl(){
		def server = new spark.HttpServer(new java.io.File("/tmp"))
		server.start()
		System.setProperty("spark.repl.class.uri", server.uri())
	}

	static void main(String[] args){

		def base_analytic_record_header = [
			"base_analytic_record_placement_id",
			"base_analytic_record_date",
			"base_analytic_record_impressions",
			"base_analytic_record_clicks",
			"base_analytic_record_cost",
			"base_analytic_record_avg_cpc",
			"base_analytic_record_avg_pos",
			"base_analytic_record_event1",
			"base_analytic_record_event2",
			"base_analytic_record_event3",
			"base_analytic_record_event4",
			"base_analytic_record_event5",
			"base_analytic_record_event6",
			"base_analytic_record_event7",
			"base_analytic_record_event8",
			"base_analytic_record_event9",
			"base_analytic_record_event10",
			"base_analytic_record_custom1",
			"base_analytic_record_custom2",
			"base_analytic_record_custom3",
			"base_analytic_record_custom4",
			"base_analytic_record_custom5",
			"base_analytic_record_custom6",
			"base_analytic_record_custom7",
			"base_analytic_record_custom8",
			"base_analytic_record_custom9",
			"base_analytic_record_custom10",
			"base_analytic_record_bid",
			"base_analytic_record_quality_score",
			"base_analytic_record_reserve",
			"base_analytic_record_change_source_name",
			"base_analytic_record_customer_group_id"
		]
		
		def cost_archive_header = ["cost_archive_group_id",
								   "cost_archive_server_campaign_fk",
								   "cost_archive_campaign_name",
								   "cost_archive_server_ad_group_fk",
								   "cost_archive_ad_group_name",
								   "cost_archive_server_keyword_fk",
								   "cost_archive_keyword_name",
								   "cost_archive_match_type_name",
								   "cost_archive_date",
								   "cost_archive_impressions",
								   "cost_archive_clicks",
								   "cost_archive_cost",
								   "cost_archive_avg_cpc",
								   "cost_archive_avg_pos",
								   "cost_archive_bid",
								   "cost_archive_quality_score",
								   "cost_archive_reserve",
								   "cost_archive_destination_url",
								   "cost_archive_customer_group_id"]


		def event_archive_header = ["event_archive_group_id",
					  "event_archive_date",
					  "event_archive_ad_server_name",
					  "event_archive_server_account_fk",
					  "event_archive_account_name",
					  "event_archive_server_campaign_fk",
					  "event_archive_campaign_name",
					  "event_archive_server_ad_group_fk",
					  "event_archive_ad_group_name",
					  "event_archive_server_keyword_fk",
					  "event_archive_keyword_name",
					  "event_archive_match_type_name",
					  "event_archive_external_placement_fk",
					  "event_archive_event_name",
					  "event_archive_event_value",
					  "event_archive_update_datetime",
					  "event_archive_external_transaction_fk",
					  "event_archive_customer_group_id"]


		setupGroovyRepl()
		def sc = new JavaSparkContext("spark://lubuntu64-1.example.local:7077", "groovySpark", "/home/appadmin/spark", 
									  [
										  "target/libs/groovy-spark-example-2.2.0-SNAPSHOT.jar",
										  "libs/repl.jar",
										  "libs/repl.jar",
										  "libs/groovy-all-2.2.0-SNAPSHOT.jar",
										  "libs/groovycsv-1.0.jar",
										  "libs/opencsv-2.1.jar",
										  "libs/joda-time-2.1.jar"
									  ]		 as String[]);
		
		def c = sc.textFile("hdfs://lubuntu64-1.example.local:8020/user/hive/warehouse/cost_archive_partitioned_by_customer_group_id/cost_archive_customer_group_id=1171773/000171_0")
		def e = sc.textFile("hdfs://lubuntu64-1.example.local:8020/user/hive/warehouse/event_archive_partitioned_by_customer_group_id/event_archive_customer_group_id=1171773/000040_0")
		PairF func = createCostArchiveKeyFunction(cost_archive_header)
		PairF eventFunc = createEventArchiveKeyFunction(event_archive_header)
		def cost_tuple = c.map(func)
		def event_tuple = e.map(eventFunc)
		def eventTypeSpecificityMap = getEventTypeSpecificityFields()
		F filterForEventType = createFilterForEventTypeFunction(event_archive_header, eventTypeSpecificityMap)
		def cog = event_tuple.cogroup(cost_tuple)
		stuff(cog)
	}

	public static void stuff(cog){
		def bill = cog
		repl()
	}
}



