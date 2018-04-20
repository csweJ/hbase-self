package com.cswe.hbase.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseExample {
	
	private final static Log log=LogFactory.getLog(HbaseExample.class);
	
	private TableName tableName=null;
	private Connection conn=null;
	private Configuration conf=null;
	
	public HbaseExample(Configuration conf) throws IOException{
		this.conf=conf;
		this.conn=ConnectionFactory.createConnection(conf);
		this.tableName=TableName.valueOf("hbase_sample_table");
	}
	
	/**
	 * 创建表
	 */
	public void createTable(){
		log.info("Hbase 表创建");
		//1.创建表描述符
		HTableDescriptor htd=new HTableDescriptor(tableName);
		//2.创建列族描述符
		HColumnDescriptor hcd=new HColumnDescriptor("info");
		//3.设置编码算法，HBase提供了DIFF，FAST_DIFF，PREFIX和PREFIX_TREE四种编码算法
		hcd.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
		//4.设置文件压缩方式  hbase提供两种压缩方式：GZ和SNAPPY  
		//GZ:有很高的压缩率但是压缩和解压缩效率低 适合冷数据
		//SNAPP:有比较低的压缩率但是压缩和解压缩效率高 适合热数据
		hcd.setCompressionType(Compression.Algorithm.SNAPPY);
		
		//5.添加列族描述到表描述中
		htd.addFamily(hcd);
		
		//6.初始化admin
		Admin admin=null;
		try {
			// 获取admin对象   
		    // Admin提供了建表、创建列族、检查表是否存在、修改表结构和列族结构以及删除表等功能。
			admin=conn.getAdmin();
			
			//判断表是否存在
			if(admin.tableExists(tableName)){
				log.warn("表已经存在");
				return;
			}
			
			admin.createTable(htd);
			ClusterStatus clusterStatus=admin.getClusterStatus();
			log.info(clusterStatus);
			log.info(admin.listNamespaceDescriptors());
			log.info("Table created successfully");
			
			
		} catch (IOException e) {
			log.error("创建表失败", e);
		}finally{
			if(admin!=null){
				try {
					admin.close();
				} catch (IOException e) {
					log.error("Failed to close admin",e);
				}
			}
		}
		
		log.info("Exiting testCreateTable.");
	}
	
	/**
	 * 删除表
	 */
	public void dropTable(){
		log.info("Hbase 删除表");
		Admin admin=null;
		try {
			admin=conn.getAdmin();
			if(admin.tableExists(tableName)){
				//只有表被disable时，才能被删除掉，所以deleteTable常与disableTable，
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(admin!=null){
				try {
					admin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	/**
	 * 多点分割  分割操作只对空Region起作用。
	 * 使用multiSplit进行多点分割将HBase表按照“A~D”、“D~F”、“F~H”、“H~Z”分为四f个Region。
	 */
	public void multiSplit(){
		Table table=null;
		Admin admin=null;
		
		try {
			admin=conn.getAdmin();
			//初始化table对象
			table=conn.getTable(tableName);
			
			Set<HRegionInfo> regionInfos=new HashSet<HRegionInfo>(); 
			
			List<HRegionLocation> regionLocations= conn.getRegionLocator(tableName).getAllRegionLocations();
			for(HRegionLocation hrl:regionLocations){
				regionInfos.add(hrl.getRegionInfo());
			}
			byte[][] sk = new byte[4][];
		    sk[0] = "J".getBytes();
		    sk[1] = "M".getBytes();
		    sk[2] = "P".getBytes();
		    sk[3] = "T".getBytes();
			for(HRegionInfo regionInfo:regionInfos){
				((HBaseAdmin)admin).multiSplit(regionInfo.getRegionName(), sk);
			}
			
			log.info("分割成功");
		} catch (IOException e) {
			 log.error("MultiSplit failed ", e);
		} catch (InterruptedException e) {
			log.error("MultiSplit failed ", e);
		}finally{
			try {
				if(table!=null){
					table.close();
				}
				if(admin!=null){
					admin.close();
				}
			} catch (IOException e) {
				log.error("Close resource failed ", e);
			}
			
		}
	}
	
	/**
	 * hbase �������ݣ�hbase �����е����ݿ⣬һ�����ݿ��ܶ�Ӧ������壬һ��������ܶ�Ӧ�����
	 * д�����ݵ�ʱ����Ҫָ��Ҫд��������������ͨ��Put����װһ������
	 */
	public void putData(){
		//ָ��������
		byte []familyName=Bytes.toBytes("info");
		//����
		byte[][] qualifiers={Bytes.toBytes("name"),Bytes.toBytes("sex"),Bytes.toBytes("age"),
				Bytes.toBytes("adress")};
		Table table =null;
		Put put=null;
		try {
			table=conn.getTable(tableName);
			List<Put> puts=new ArrayList<Put>();
			//new Put��������װһ�е�����
			put =new Put(Bytes.toBytes("2017122601"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("zhang san"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("F"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("18"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("wuhan"));
			puts.add(put);
			
			put =new Put(Bytes.toBytes("2017122602"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("li si"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("M"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("17"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("wuhan"));
			puts.add(put);
			
			put =new Put(Bytes.toBytes("2017122603"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("wang wu"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("F"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("20"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("beijing"));
			puts.add(put);
			
			put =new Put(Bytes.toBytes("2017122604"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("zhao liu"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("F"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("20"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("nanjing"));
			puts.add(put);
			
			put =new Put(Bytes.toBytes("2017122605"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("xiao he"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("F"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("20"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("beijing"));
			puts.add(put);
			
			put =new Put(Bytes.toBytes("2017122606"));
			put.addColumn(familyName, qualifiers[0], Bytes.toBytes("xiao xin"));
			put.addColumn(familyName, qualifiers[1], Bytes.toBytes("F"));
			put.addColumn(familyName, qualifiers[2], Bytes.toBytes("20"));
			put.addColumn(familyName, qualifiers[3], Bytes.toBytes("beijing"));
			puts.add(put);
			table.put(puts);
			
			log.info("�������ݳɹ�");
		} catch (IOException e) {
			log.error("Put failed ", e);
			e.printStackTrace();
		}finally{
			if(table!=null){
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * ɾ������
	 */
	public void deleteData(){
		
		Table table=null;
		//�йؼ�ֵ������������
		byte [] rowKey=Bytes.toBytes("2017122603");
		try {
			table=conn.getTable(tableName);
			
			//Delete ����������װɾ��������
			Delete deleteObj=new Delete(rowKey);
			
			table.delete(deleteObj);
		} catch (IOException e) {
			log.error("ɾ������ʧ��");
			e.printStackTrace();
		}finally{
			if(table!=null){
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * ������������������������֧���޸ģ������޸ģ�ɾ���ɵ��ٴ����µ�
	 */
	public void createIndex(){
		
		String indexName="index_name";
		//1.��������ʵ��
		IndexSpecification indexSpec=new IndexSpecification(indexName);
		
		//2.ȷ������Щ�ֶ����洴������
		indexSpec.addIndexColumn(new HColumnDescriptor("info"),"name", ValueType.String);
		
		//Hbase ֧���ڶ���ֶ��ϴ������������Ǵ�����������ʱ��Ҫָ��ÿ���еĳ���
//		IndexSpecification indexUnite=new IndexSpecification("index_unite");
//		indexUnite.addIndexColumn(new HColumnDescriptor("info"), "name", ValueType.String, 20);
//		indexUnite.addIndexColumn(new HColumnDescriptor("info"), "name", ValueType.String, 3);
		
		//3.������������
		IndexAdmin iAdmin=null;
		Admin admin=null;
		
		try {
			iAdmin=new IndexAdmin(conf);
			
			
			//4.������������
			iAdmin.addIndex(tableName, indexSpec);
			
			//ָ�������е������ͼ�������
			admin=conn.getAdmin();
			
			//5.��ȡ������
			HTableDescriptor htd=admin.getTableDescriptor(tableName);
			//6.disabled ��
			admin.disableTable(tableName);
			htd=admin.getTableDescriptor(tableName);
			//ʵ��������������
			HColumnDescriptor indexColDesc=new HColumnDescriptor(IndexMasterObserver.DEFAULT_INDEX_COL_DESC);
		
			//����û��ڴ�����ʱ���л����������˼��ܣ���ô�ڴ�����Ӧ������ʱ����Ҫ��Ӷ�Ӧ�ļ��ܲ���
			//indexColDesc.setEncryptionType("AES");
			
			htd.setValue(Constants.INDEX_COL_DESC_BYTES, indexColDesc.toByteArray());
			//7.�޸ı�
			admin.modifyTable(tableName, htd);
			//8.enabled ��
			admin.enableTable(tableName);
			
			log.info("���������ɹ�");
		} catch (IOException e) {
			log.info("��������ʧ��");
			e.printStackTrace();
		}finally{
			try {
				if(admin!=null){
					admin.close();
				}
				if(iAdmin!=null){
					iAdmin.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * ���ڶ���������ѯ
	 */
	public void scanDataByIndex(){
		log.info("���ڶ�������ɨ������");
		
		Table table=null;
		ResultScanner resultScanner=null;
		try {
			//1.ʵ����tableʵ��
			table=conn.getTable(tableName);
			//2.����scan����
			Scan scan=new Scan();
			//�����������ϵĹ���������
			Filter filter=new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"), CompareOp.EQUAL, "zhang san".getBytes());
			scan.setFilter(filter);
			//3.ɨ�����Ķ������ݱ�����ResultScanner������,ÿ��������result�������ʽ�洢��Result�д洢���cell
			resultScanner= table.getScanner(scan);
			for(Result result:resultScanner){
				for(Cell cell:result.rawCells()){
					log.info(Bytes.toString(CellUtil.cloneRow(cell))+":"
							+Bytes.toString(CellUtil.cloneFamily(cell))+","
							+Bytes.toString(CellUtil.cloneQualifier(cell))+","
							+Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			
			 log.info("Scan data by index successfully.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(resultScanner!=null){
				resultScanner.close();
			}
			
			if(table!=null){
				try {
					table.close();
				} catch (IOException e) {
					log.error("Close table failed ", e);
				}
			}
		}
		
	}
	
	/**
	 * ʹ��Get��������ȡ����
	 */
	public void getData(){
		Table table=null;
		
		byte [] rowKey=Bytes.toBytes("2017122602");
		byte [] familyName=Bytes.toBytes("info");
		byte [][] qualifier={Bytes.toBytes("name"),Bytes.toBytes("adress")};
		
		try {
			//1.ʵ�����ñ��Ӧ��tableʵ��
			table=conn.getTable(tableName);
			//2.ͨ��rowkeyʵ���� Get���� 
			Get get=new Get(rowKey);
			
			//3.�趨��ѯ���� ��������������
			get.addColumn(familyName, qualifier[0]);
			get.addColumn(familyName, qualifier[1]);
			
			//4.�ύGet��ѯ������ ,��ѯ����ĸ������ݱ�����Result�С�Result�д洢�˶��Cell�����������Ϣ��
			Result result = table.get(get);
			
			//5.����cell  
			for(Cell cell:result.rawCells()){
				 log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
				            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
				            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
				            + Bytes.toString(CellUtil.cloneValue(cell)));

			}
			log.info("Get data successfully.");

			
		} catch (IOException e) {
			log.error("get data failed ", e);
		}finally{
			try {
				if(table!=null){
					table.close();
				}
			} catch (IOException e) {
				log.error("Close table failed ", e);
			}
		}
	}
	
	/**
	 * ͨ��Scan ������ɨ���ȡ����
	 */
	public void scanData(){
		Table table=null;
		ResultScanner resultScanner=null;
		try {
			//1.ʵ����table����
			table =conn.getTable(tableName);
			//2.ʵ����scan ����
			Scan scan=new Scan();
			//3.����scan �����ѯ����
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
			
			//4.��������Batch��Caching ����
			//batch:ʹ��scan����next�ӿ�ÿ����󷵻صļ�¼������һ�ζ�ȡ�������
			//caching:һ��RPC��ѯ�������ķ��ص�next��Ŀ����һ��RPC��ȡ�������й�
			scan.setCaching(1000);
			
			//5.�ύscan ��ѯ����  ,ɨ�����Ķ������ݱ�����ResultScanner������,ÿ��������result�������ʽ�洢��Result�д洢���cell
			resultScanner =table.getScanner(scan);
			
			//6.������ӡ
			for(Result r=resultScanner.next();r!=null;r=resultScanner.next()){
				for(Cell cell:r.rawCells()){
					  log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
				              + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
				              + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
				              + Bytes.toString(CellUtil.cloneValue(cell)));
				}
				
			}
			
			 log.info("Scan data successfully.");
		} catch (IOException e) {
			log.error("Scan data failed ", e);
		}finally{
			if(resultScanner!=null){
				resultScanner.close();
			}
			try {
				if(table!=null){
					table.close();
				}
			} catch (IOException e) {
				log.error("close failed ", e);
			}
		}
	}
	
	/**
	 * filter:��Ҫ����Scan��Get�����н������ݹ��ˣ�
	 */
	public void valueFilter(){
		Table table=null;
		ResultScanner resultScanner=null;
		try {
			//1.ʵ����table����
			table=conn.getTable(tableName);
			//2.����scan����
			Scan scan=new Scan();
			//3.��Ӳ��ҵ���
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
			scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));
			
			//4.����һ��������
			SingleColumnValueFilter filter=new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("sex"),
					CompareOp.EQUAL, Bytes.toBytes("F"));
			
			//���߶���������������ϵΪand��
//			FilterList filterList=new FilterList(Operator.MUST_PASS_ALL);
//			filterList.addFilter(filter);
//			scan.setFilter(filterList);
			//5.���ù�����
			scan.setFilter(filter);
			
			//6.�ύscan ��ѯ
			resultScanner=table.getScanner(scan);
			
			//7.ѭ������resultScanner
			for(Result r=resultScanner.next();r!=null;r=resultScanner.next()){
				for(Cell cell:r.rawCells()){
			          log.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
			                  + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
			                  + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
			                  + Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			
			
		} catch (IOException e) {
			log.error("Scan data failed ", e);
		}finally{
			if(resultScanner!=null){
				resultScanner.close();
			}
			
			try {
				if(table!=null){
					table.close();
				}
			} catch (IOException e) {
				log.error("close failed ", e);
			}
		}
	}
	
	/**
	 * �޸ı��������
	 */
	public void modifyTable(){
		Admin admin=null;
		byte []family=Bytes.toBytes("education");
		try {
			admin=conn.getAdmin();
			//��ȡ��ǰ������
			HTableDescriptor htd=admin.getTableDescriptor(tableName);
			//�ж������Ƿ��Ѿ�����
			if(!htd.hasFamily(family)){
				HColumnDescriptor hcd=new HColumnDescriptor(family);
				htd.addFamily(hcd);
				
				admin.disableTable(tableName);
				admin.modifyTable(tableName, htd);
				admin.enableTable(tableName);
			}
			  log.info("Modify table successfully.");
		} catch (IOException e) {
			 log.error("Modify table failed ", e);
		}finally{
			try {
				if(admin!=null){
					admin.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * ɾ��������ͨ��indexAdmin ����������
	 */
	public void dropIndex(){
		IndexAdmin iAdmin=null;
		try {
			//ʵ����IndexAdminʵ��
			iAdmin=new IndexAdmin(conf);
			
			//ɾ����������
			iAdmin.dropIndex(tableName, "index_name");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(iAdmin!=null){
					iAdmin.close();
				}
			} catch (IOException e) {
				log.error("close iAdmin failed");
			}
		}
	}
	
	 /**
	   * mob���ݶ�
	   */
	  public void testMOBDataRead() {
	    log.info("Entering testMOBDataRead.");
	    ResultScanner scanner = null;
	    Table table = null;
	    Admin admin = null;
	    try {

	      //ʵ����table
	      table = conn.getTable(tableName);
	      admin = conn.getAdmin();
	      admin.flush(table.getName());
	      
	      Scan scan = new Scan();
	      // get table scanner
	      scanner = table.getScanner(scan);
	      for (Result result : scanner) {
	        byte[] value = result.getValue(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"));
	        String string = Bytes.toString(value);
	        log.info("value:" + string);
	      }
	      log.info("MOB data read successfully.");
	    } catch (Exception e) {
	      log.error("MOB data read failed ", e);
	    } finally {
	      if (scanner != null) {
	        scanner.close();
	      }
	      if (table != null) {
	        try {
	          // Close table object
	          table.close();
	        } catch (IOException e) {
	          log.error("Close table failed ", e);
	        }
	      }
	      if (admin != null) {
	        try {
	          // Close the Admin object.
	          admin.close();
	        } catch (IOException e) {
	          log.error("Close admin failed ", e);
	        }
	      }
	    }
	    log.info("Exiting testMOBDataRead.");
	  }

	  /**
	   * mob���ݵ�д��
	   */
	  public void testMOBDataInsertion() {
	    log.info("Entering testMOBDataInsertion.");

	    Table table = null;
	    try {
	      // ��������
	      Put p = new Put(Bytes.toBytes("row"));
	      byte[] value = new byte[1000];
	  
	      //����mob���ʱ��������mobcf�Ͽ�����mob���ܣ� �ڸ����������cf1��
	      p.addColumn(Bytes.toBytes("mobcf"), Bytes.toBytes("cf1"), value);
	    //table ʵ��
	      table = conn.getTable(tableName);
	      // ������
	      table.put(p);
	      log.info("MOB data inserted successfully.");

	    } catch (Exception e) {
	      log.error("MOB data inserted failed ", e);
	    } finally {
	      if (table != null) {
	        try {
	          table.close();
	        } catch (Exception e1) {
	          log.error("Close table failed ", e1);
	        }
	      }
	    }
	    log.info("Exiting testMOBDataInsertion.");
	  }

	  /**
	   * HBase MOB���ݵ�д������ͨHBase���ݵĶ���û��ʲô����Կͻ���˵��͸���ġ�
	   * Ϊ��ʹ��HBase MOB������Ҫ��hbase-site.xml�����HBase MOB��ص�������
	   * ����֮�⻹��Ҫ��ָ��column family�Ͽ���mob���ܣ������봴����ͨ��һ��
	   */
	  public void testCreateMOBTable() {
	    log.info("Entering testCreateMOBTable.");

	    Admin admin = null;
	    try {
	      // Create Admin instance
	      admin = conn.getAdmin();
	      HTableDescriptor tabDescriptor = new HTableDescriptor(tableName);
	      HColumnDescriptor mob = new HColumnDescriptor("mobcf");
	      // �����忪��MOB����
	      mob.setMobEnabled(true);
	      // ���� mob ����ֵ
	      mob.setMobThreshold(10L);
	      
	      tabDescriptor.addFamily(mob);
	      admin.createTable(tabDescriptor);
	      log.info("MOB Table is created successfully.");

	    } catch (Exception e) {
	    	log.error("MOB Table is created failed ", e);
	    } finally {
	      if (admin != null) {
	        try {
	          // Close the Admin object.
	          admin.close();
	        } catch (IOException e) {
	        	log.error("Close admin failed ", e);
	        }
	      }
	    }
	    log.info("Exiting testCreateMOBTable.");
	  }
	
}
