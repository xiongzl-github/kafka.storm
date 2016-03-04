package kafka.storm.kafka.storm;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SenqueceBolt extends BaseBasicBolt{
	
	private Connection con = null;
	private Statement  st = null;
	public static int totalCount = 0;
	public static int handleCount = 0;
	
    
    public void execute(Tuple input, BasicOutputCollector collector) {
         String word = (String) input.getValue(0);
         if (totalCount++ != 0) {
        	 dataHandle(word.split(","));
		 }
         
         collector.emit(new Values(word));
    }
    
    public void dataHandle(String[] datas){
    	if (datas[5] == null || "".equals(datas[5].trim())) {
			return;
		}
    try {
    	if (Double.valueOf(datas[5])>1000) {
    		StringBuilder sql = new StringBuilder("insert into welcome_product (product_name,product_code,deal_price,total_price,amount) values(");
    		sql.append("'"+datas[1]+"',");
    		sql.append("'"+datas[2]+"',");
    		if (datas[4]==null || "".equals(datas[4].trim())) {
    			sql.append("0.00,");
			}else {
				sql.append( new BigDecimal(Double.valueOf(datas[4]))+",");
			}
    		if (datas[5]==null || "".equals(datas[5].trim())) {
    			sql.append("0.00,");
    		}else {
    			sql.append( new BigDecimal(Double.valueOf(datas[5]))+",");
    		}
    		if (datas[6]==null || "".equals(datas[6].trim())) {
    			sql.append("0,");
    		}else {
    			sql.append(Integer.valueOf(datas[6])+"");
    		}
    		
    		sql.append(")");
    		
    		insert(sql.toString());
    		handleCount++;
		}
	} catch (Exception e) {
		e.printStackTrace();
	}
    	
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
    
    public  Connection getConnection(){
    	Connection con = null;
    	try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/hadoop?useUnicode=true&characterEncoding=utf-8", "trade", "trade");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return con;
    } 
    
    public  void insert(String sql){
    	 con = getConnection();
    	 try {
    		 System.err.println(">>>SQL:"+sql);
			st = con.createStatement();
	    	int count = st.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
    	
    }
    
}