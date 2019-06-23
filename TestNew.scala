package com.selva.example

/*import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Put,HTable}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog*/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import scala.xml.XML
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, DataFrameReader }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object TestNew {

  case class audit(rowKey: String, timeStamp: String, processed: String)

case class PndsMdm(rowID: String,classificationCode: String, dataSourceType: String, specialtyBoardCertificationCode: String, providerIDltk: String, timeStamp: String, classificationprimSpclInd: String, classificationeffectivedate: String, classificationcanceldate: String, classificationspclBrdcertdate: String, classificationspclBrdExpDate: String, specialtyCode_list: scala.collection.mutable.ListBuffer[String], primSpclInd_list: scala.collection.mutable.ListBuffer[String], spclBrdCertTypCode_list: scala.collection.mutable.ListBuffer[String],  spclBrdCertDate_list: scala.collection.mutable.ListBuffer[String], specialtyeffectivedate_list: scala.collection.mutable.ListBuffer[String], specialtycanceldate_list: scala.collection.mutable.ListBuffer[String], spclBrdExpDate_list: scala.collection.mutable.ListBuffer[String], specialtyInd_list: scala.collection.mutable.ListBuffer[String], NPICode_list: scala.collection.mutable.ListBuffer[String], NPIEffectiveDate_list: scala.collection.mutable.ListBuffer[String], NPICancelDate_list: scala.collection.mutable.ListBuffer[String])
                     
case class Pnds(rowID: String,classificationCode: String, dataSourceType: String, specialtyBoardCertificationCode: String, providerIDltk: String, timeStamp: String, classificationprimSpclInd: String, classificationeffectivedate: String, classificationcanceldate:String, classificationspclBrdcertdate: String, classificationspclBrdExpDate: String, specialtyCode_list: String, primSpclInd_list:String, spclBrdCertTypCode_list:String, spclBrdCertDate_list:String, specialtyeffectivedate_list:String, specialtycanceldate_list:String, spclBrdExpDate_list:String,specialtyInd_list:String, NPICode_list: String, NPIEffectiveDate_list: String, NPICancelDate_list: String)

case class Pnd(classificationCode: String, dataSourceType: String, specialtyBoardCertificationCode: String, classificationprimSpclInd: String,
    classificationeffectivedate: String, classificationcanceldate: String, classificationspclBrdcertdate: String, classificationspclBrdExpDate: String, 
    specialtyCode_list: scala.collection.mutable.ListBuffer[String], primSpclInd_list: scala.collection.mutable.ListBuffer[String],
    spclBrdCertTypCode_list: scala.collection.mutable.ListBuffer[String],  spclBrdCertDate_list: scala.collection.mutable.ListBuffer[String],
    specialtyeffectivedate_list: scala.collection.mutable.ListBuffer[String], specialtycanceldate_list: scala.collection.mutable.ListBuffer[String], 
    spclBrdExpDate_list: scala.collection.mutable.ListBuffer[String], specialtyInd_list: scala.collection.mutable.ListBuffer[String])

case class PndList(rowID: String, providerIDltk: String, timeStamp: String,
    pndList: scala.collection.mutable.MutableList[Pnd], NPICode_list: scala.collection.mutable.ListBuffer[String], 
    NPIEffectiveDate_list: scala.collection.mutable.ListBuffer[String], NPICancelDate_list: scala.collection.mutable.ListBuffer[String])

case class xmlData(id:String, rawXml:String) 

   def parseXML(rowId:String, in: String) =
    {
       //val xml = scala.xml.XML.loadString(in)
      var timeStamp = ""
     val xml = XML.loadFile("C:/Users/hp/Desktop/sample.xml")

      var PndsMdmList = scala.collection.mutable.MutableList[PndList]()
      
            var pndList = scala.collection.mutable.MutableList[Pnd]()


      val isclassifyCode = (xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "professional" \\ "hcpTaxonomy" \\ "classification" \\ "code").nonEmpty
      if (isclassifyCode) {
        val classification = xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "professional" \\ "hcpTaxonomy" \\ "classification"
        for (src_classification <- classification) {
          var classficiationTaxonomy = src_classification \ "code"
          var classificationCode = classficiationTaxonomy.text.trim()

          var dataSource = src_classification \ "dataSourceType"
          var dataSourceType = dataSource.text.trim()

          var specialtyBoardCertification = src_classification \ "specialtyBoardCertificationCode" \ "code"
          var specialtyBoardCertificationCode = specialtyBoardCertification.text.trim()

          var classificationprimSpcl = src_classification \ "primClassInd"
          var classificationprimSpclInd = classificationprimSpcl.text.trim()

          var classificationeffectivestartdate = src_classification \ "effDateRange" \ "effStartDate"
          var classificationeffectivedate = classificationeffectivestartdate.text.trim()
          
          var classificationeffectivecanceldate = src_classification \ "effDateRange" \ "effCancelDate"
          var classificationcanceldate = classificationeffectivecanceldate.text.trim()
          
          var classificationspclBrdcertificatedate = src_classification \ "spclBrdCertDate"
          var classificationspclBrdcertdate = classificationspclBrdcertificatedate.text.trim()

          var classificationspclBrdExpirationDate = src_classification \ "spclBrdExpDate"
          var classificationspclBrdExpDate = classificationspclBrdExpirationDate.text.trim()

          var specialtyCode_list = scala.collection.mutable.ListBuffer.empty[String]
          var primSpclInd_list = scala.collection.mutable.ListBuffer.empty[String]
          var spclBrdCertTypCode_list = scala.collection.mutable.ListBuffer.empty[String]
          var spclBrdCertDate_list = scala.collection.mutable.ListBuffer.empty[String]
          var specialtyeffectivedate_list = scala.collection.mutable.ListBuffer.empty[String]
          var specialtycanceldate_list = scala.collection.mutable.ListBuffer.empty[String]
          var spclBrdExpDate_list = scala.collection.mutable.ListBuffer.empty[String]
          var specialtyInd_list = scala.collection.mutable.ListBuffer.empty[String]

          for (specialty <- src_classification \\ "specialty") {
            var specialtyCode = specialty \ "code"
            specialtyCode_list += specialtyCode.text.trim()

            var primSpclInd = specialty \ "primSpclInd"
            primSpclInd_list += primSpclInd.text.trim()

            var spclBrdCertTypCode = specialty \ "spclBrdCertType" \ "code"
            spclBrdCertTypCode_list += spclBrdCertTypCode.text.trim()

            var spclBrdCertDate = specialty \ "spclBrdCertDate"
            spclBrdCertDate_list += spclBrdCertDate.text.trim()

            var spclBrdExpDate = specialty \ "spclBrdExpDate"
            spclBrdExpDate_list += spclBrdExpDate.text.trim()

            var specialtyeffectivedate = specialty \ "effDateRange" \ "effStartDate"
            specialtyeffectivedate_list += specialtyeffectivedate.text.trim()
            
            var specialtycanceldate = specialty \ "effDateRange" \ "effCancelDate"
            specialtycanceldate_list += specialtycanceldate.text.trim()
            
            var specialtyInd = specialty \ "practicingSpecialtyInd"
            specialtyInd_list += specialtyInd.text.trim()
            
           pndList += Pnd(classificationCode,dataSourceType,specialtyBoardCertificationCode,classificationprimSpclInd,classificationeffectivedate,
                classificationcanceldate,classificationspclBrdcertdate,classificationspclBrdExpDate,specialtyCode_list,
                primSpclInd_list,spclBrdCertTypCode_list,spclBrdCertDate_list,spclBrdExpDate_list,specialtyeffectivedate_list,specialtycanceldate_list,specialtyInd_list)
          }

          ///msg:ProvMasterMsg/msg:body/msg:provider/msg:providerIDObj/msg:enterpriseID/value
          var providerIDltk = ""
          var enterpriseID_node = xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "providerIDObj" \\ "enterpriseID" \ "value"
          providerIDltk = enterpriseID_node.text

          ///msg:ProvMasterMsg/msg:headerObj/timeStamp
          var timeStamp_node = xml \\ "ProvMasterMsg" \\ "headerObj" \\ "timeStamp"
          timeStamp = timeStamp_node.text
          
          var NPICode_list = scala.collection.mutable.ListBuffer.empty[String]
          var NPIEffectiveDate_list = scala.collection.mutable.ListBuffer.empty[String]
          var NPICancelDate_list = scala.collection.mutable.ListBuffer.empty[String]
          
          val NPI = xml \\ "ProvMasterMsg" \\ "body" \\ "provider" \\ "professional" \\ "hCPIDObj" \\ "nonValidatedNPI" \\ "taxonomy"
          for (NPI_taxonomy <- NPI ){
         
          ///msg:ProvMasterMsg/msg:body/msg:provider/msg:professional/msg:hCPIDObj/msg:nonValidatedNPI/msg:taxonomy/msg:code/code
          var NPICode = ""
          var npi_node = NPI_taxonomy \ "code" \ "code"
          NPICode_list += npi_node.text.trim()
          
          var NPIEffectiveDate = ""
          var NPIEffectiveDate_node = NPI_taxonomy \ "taxonomyEffDates" \ "effStartDate"
          NPIEffectiveDate_list += NPIEffectiveDate_node.text.trim()
          
          var NPICancelDate = ""
          var NPICancelDate_node = NPI_taxonomy \ "taxonomyEffDates" \ "effCancelDate"
          NPICancelDate_list += NPICancelDate_node.text.trim()
          }
          
          
        PndsMdmList +=  PndList(rowId,providerIDltk, timeStamp,pndList,NPICode_list,NPIEffectiveDate_list,NPICancelDate_list)
       //   PndsMdmList += PndsMdm(rowId, classificationCode, dataSourceType, specialtyBoardCertificationCode, providerIDltk, timeStamp, classificationprimSpclInd, classificationeffectivedate, classificationcanceldate, classificationspclBrdcertdate, classificationspclBrdExpDate, specialtyCode_list, primSpclInd_list, spclBrdCertTypCode_list, spclBrdCertDate_list, specialtyeffectivedate_list, specialtycanceldate_list, spclBrdExpDate_list,specialtyInd_list,NPICode_list,NPIEffectiveDate_list,NPICancelDate_list)
           
        }
      }
     println("PndsMdmList.size"+PndsMdmList)
      PndsMdmList;
    }
 
  def getResult(row : PndList)  = {
    
               var pndsList = scala.collection.mutable.IndexedSeq.empty[Pnds]
               var PndsMdmList = scala.collection.mutable.MutableList[Pnds]()
               val rowID = row.rowID
              
               val providerIDltk = row.providerIDltk
               val timeStamp = row.timeStamp 
                for(row <-row.pndList)
               {
               val classificationCode = row.classificationCode  
               val dataSourceType = row.dataSourceType
               val specialtyBoardCertificationCode = row.specialtyBoardCertificationCode  
               val classificationprimSpclInd = row.classificationprimSpclInd
               val classificationeffectivedate = row.classificationeffectivedate  
               val classificationcanceldate = row.classificationcanceldate
               val classificationspclBrdcertdate = row.classificationspclBrdcertdate
               val classificationspclBrdExpDate = row.classificationspclBrdExpDate

               val specialtyCode = row.specialtyCode_list
               val primSpclInd = row.primSpclInd_list
               val spclBrdCertTypCode = row.spclBrdCertTypCode_list
               val spclBrdCertDate = row.spclBrdCertDate_list
               
               val specialtyeffectivedate = row.specialtyeffectivedate_list
               val specialtycanceldate = row.specialtycanceldate_list
               val spclBrdExpDate = row.spclBrdExpDate_list
               val specialtyInd = row.specialtyInd_list
               
                if(!specialtyCode.isEmpty )
               {
                for( i <- 0 until specialtyCode.size)  {
                  
                  val seq = Pnds(rowID,classificationCode,dataSourceType,
                   specialtyBoardCertificationCode,providerIDltk,timeStamp,classificationprimSpclInd,classificationeffectivedate,
                   classificationcanceldate,classificationspclBrdcertdate,classificationspclBrdExpDate,specialtyCode(i),primSpclInd(i),
                   spclBrdCertTypCode(i),spclBrdCertDate(i),specialtyeffectivedate(i),specialtycanceldate(i),spclBrdExpDate(i),specialtyInd(i),
                   "","","")
                   PndsMdmList +=seq
                }
              
               }else
               {
                 val sequ = Pnds(rowID,classificationCode,dataSourceType,
                   specialtyBoardCertificationCode,providerIDltk,timeStamp,classificationprimSpclInd,classificationeffectivedate,
                   classificationcanceldate,classificationspclBrdcertdate,classificationspclBrdExpDate,"","","","","","","","",
                   "","","")
                    PndsMdmList +=sequ
               }
               
               }
               val NPICode = row.NPICode_list
               val NPIEffectiveDate = row.NPIEffectiveDate_list 
               val NPICancelDate = row.NPICancelDate_list
               
               
             println("NPICode"+NPICode)
             

              
             //NPIcode()
               //}
              //def NPIcode(NPICode: String, NPIEffectiveDate: String, NPICancelDate: String): String={
               if(!NPICode.isEmpty )
               {
               for(i <- 0 until NPICode.size){
               
                 val seq1 =  Pnds(rowID,"","","",providerIDltk,timeStamp,"","","","","","","","","","","","","",
                   NPICode(i),NPIEffectiveDate(i),NPICancelDate(i))
                   
                   PndsMdmList +=seq1
                   }
               
               }else
               {
                 val sequ1 = Pnds(rowID,"","","",providerIDltk,timeStamp,"","","","","","","","","","","","","",
                   "","","")
                  PndsMdmList +=sequ1
               }
               PndsMdmList
               }
           // }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import spark.implicits._
    val rowId = "1222"

    //  var maxRowKeyLast = spark.sql("select max(rowKey) from auditTable where processed = 'Completed' ").head().get(0)
    var maxRowKeyLast: Any = null
    if (maxRowKeyLast == null)
      maxRowKeyLast = 0

    println("maxRowKeyLast : " + maxRowKeyLast)
    var result = parseXML(rowId, "C:/Users/hp/Desktop/sample.xml")

    val t1 = result.toDS().flatMap(row => getResult(row)).filter(col("rowID") > maxRowKeyLast)
   // t1.write.mode(SaveMode.Append).saveAsTable("TableName")

    t1.show(false)

    /*    save record to pnds
*/

    val maxRowKey = result.toDS().filter(col("rowID") > maxRowKeyLast).select(max(col("rowID"))).head().get(0)

    if (maxRowKey == null) {
      println("There is no records to process")

    } else {
      println("came here")

      val rowKey = maxRowKey.toString()
      import java.time.LocalDateTime
      import java.time.format.DateTimeFormatter
      var auditList = scala.collection.mutable.MutableList[audit]()
      val timeStamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
      auditList += audit(rowKey, timeStamp, "Completed")
      val auditRec = auditList.toDS()
      auditRec.write.mode(SaveMode.Overwrite).saveAsTable("auditTable")
      val auditTable = spark.sql("select * from  auditTable")
      auditTable.show

    }

  }

}