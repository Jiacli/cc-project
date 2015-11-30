import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

public class JDBCJava {
	// JDBC driver name and database URL
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = "jdbc:mysql://localhost:3306/mydb";

	// Database credentials
	private static final String USER = "root";
	private static final String PASS = "123456";
	private static final String maxUserid = "2594997268";
	private static final String minUserid = "12";
	private static final float minId = 12;
	private static final double maxId = 2594997268.0;
	Connection conn = null;
	
	// q6 variables for plan1
	private HashMap <Integer, PriorityQueue<MyRequest>> transitMap = new HashMap<Integer, PriorityQueue<MyRequest>>();
	private HashMap <String, String> tweetMap = new HashMap<String, String>();
	private HashMap <Integer, Integer> seqMap = new HashMap<Integer, Integer>();
	private HashMap <Integer, HashSet<String>> idMap = new HashMap<Integer, HashSet<String>>();

	JDBCJava() throws SQLException {
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
	}

	public String query(String key) {

		int q1 = key.indexOf("q1");
		int q2 = key.indexOf("q2");
		int q3 = key.indexOf("q3");
		int q4 = key.indexOf("q4");
		int q5 = key.indexOf("q5");
		int q6 = key.indexOf("q6");

		// do query
		if (q2 != -1) {
			return doQ2(key);
		} else if (q3 != -1) {
			return doQ3(key);
		} else if (q4 != -1) {
			return doQ4(key);
		} else if (q5 != -1) {
			return doQ5(key);
		} else if (q6 != -1) {
			return doQ6(key);
		}
		return "shouldn't goes here!";
	}

	private String doQ5(String key) {
		Statement stmt = null;
		try {
				key = key.substring(3);
				Class.forName("com.mysql.jdbc.Driver");
				stmt = conn.createStatement();
				String[] keys = key.split(",");
				String sql,beginId,endId;
				beginId = keys[0];
				endId = keys[1];
				float begin = Float.valueOf(keys[0]);
				float end = Float.valueOf(keys[1]);
				if(begin<minId){
					beginId = minUserid;
				}
				if(end>maxId){
					endId = maxUserid;
				}

				sql = "SELECT * from test5 where userid=(select min(userid) from test5 where userid>=" +beginId+") or userid=(select max(userid) from test5 where userid<="+endId+");";
				ResultSet rs = stmt.executeQuery(sql);
				rs.next();
				int first = rs.getInt("count");
				int self = rs.getInt("self");
				rs.next();
				int second = rs.getInt("count");
				return String.valueOf(second-first+self);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			return "excepppp from q5";
	}

	private String doQ6(String key) {
		key = key.substring(3);
		String[] keys = key.split(",");
		Integer tid = Integer.parseInt(keys[0]);
		String opt = keys[1];
		if (opt.equals("s")) {
			return doStart(tid);
		} else if (opt.equals("a")) {
			Integer seq = Integer.parseInt(keys[2]);
			String tweetid = keys[3];
			String tag = key.substring(key.indexOf("tag=") + 4);
			return doAdd(tid, seq, tweetid, tag, "a");
		} else if (opt.equals("r")) {
			Integer seq = Integer.parseInt(keys[2]);
			String tweetid = keys[3];
			return doRead(tid, seq, tweetid, "r");	
		} else if (opt.equals("e")) {
			return doEnd(tid);
		}
		return null;
	}

	private String doEnd( Integer tid) {
		transitMap.remove(tid);
		seqMap.remove(tid);
		HashSet<String> idSet = idMap.remove(tid);
		if(idSet == null){
			return "0";
		}
		
		for (String id : idSet) {
			String post = tweetMap.remove(id);
			writeIntoMysql(id, post);
		}
		return "0";
	}

	private String doRead(Integer tid, Integer seq, String tweetid, String opt) {
		System.out.println("do Read");
//		PriorityQueue<MyRequest> q = transitMap.get(tid);
//		MyRequest newR = new MyRequest(seq, tweetid, null, opt);
		String result;
		
		if (tweetMap.containsKey(tweetid)) {
			result = tweetMap.get(tweetid);
		} else {
			result = readFromMysql(tweetid);
			tweetMap.put(tweetid, result);
		}
		
//		synchronized(q) {
//			q.add(newR);
//			
//			MyRequest r = q.peek();
//			while(r != newR ){//|| seqMap.get(tid) +1 != r.seq) {
//				try {
//					q.wait();
//					System.out.println("waik up!!!!!!");
//					System.out.println("seqNumber: " + seqMap.get(tid));
//					System.out.println("request seqNumber: " + r.seq);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				r = q.peek();
//			}
//			System.out.println("do read option!!!!");
//			
//			q.poll();
//			
//			seqMap.put(tid, seqMap.get(tid) + 1);
//			idMap.get(tid).add(tweetid);
//			
//			if (tweetMap.containsKey(tweetid)) {
//				result = tweetMap.get(tweetid);
//			} else {
//				result = readFromMysql(tweetid);
//				tweetMap.put(tweetid, result);
//			}
//			q.notifyAll();
//		}
		return result;
	}

	private void writeIntoMysql(String tweetid, String result) {
		Statement stmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			stmt = conn.createStatement();	
			String sql = "UPDATE test6 SET post='" + result + "' WHERE tweetid=" + tweetid + ";";
			stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private String doAdd(Integer tid, Integer seq, String tweetid, String tag, String opt) {
		System.out.println("do add");
//		PriorityQueue<MyRequest> q = transitMap.get(tid);
//		MyRequest newR = new MyRequest(seq, tweetid, tag, opt);
		
//		synchronized (q) {
//			q.add(newR);
//			
//			MyRequest r = q.peek();
//			while(r != null && r.opt.equals("a")){ //&& (seqMap.get(tid) +1 == r.seq)) {
//				q.poll();
//				
//				seqMap.put(tid, seqMap.get(tid) + 1);
//				idMap.get(tid).add(tweetid);
//				
//				if (tweetMap.containsKey(r.tweetid)) {
//					tweetMap.put(tweetid, tweetMap.get(tweetid) + r.tag);
//				} else {
//					String result = readFromMysql(tweetid);
//					tweetMap.put(tweetid, result + r.tag);
//				}
//				r = q.peek();
//			}
//			
//			if (q.peek() != null) {
//				for (MyRequest req : q) {
//					System.out.print(" req in queue: " + req.opt);
//					System.out.print(" req in queue: " + req.seq);
//					System.out.print(" req in queue: " + req.tweetid);
//					System.out.println("next seq number: " + seqMap.get(tid));
//					
//				}
//				q.notifyAll();
//			}
//			
//		}
		
		if (tweetMap.containsKey(tweetid)) {
			tweetMap.put(tweetid, tweetMap.get(tweetid) + tag);
		} else {
			String result = readFromMysql(tweetid);
			tweetMap.put(tweetid, result + tag);
		}
		
		return tag;
	}

	private String readFromMysql(String tweetid) {
		Statement stmt = null;
		String result = "";

		try {
			Class.forName("com.mysql.jdbc.Driver");
			stmt = conn.createStatement();
			String sql = "SELECT post from test6 where tweetid=" + tweetid + ";";
			ResultSet rs = stmt.executeQuery(sql);
			
			
			while (rs.next()) {
				result = rs.getString("post");
			}
			
			if (result == null) {
				result = "";
			}
			stmt.close();
			rs.close();
			

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}

	private String doStart(Integer tid) {
		transitMap.put(tid, new PriorityQueue<MyRequest>());
		seqMap.put(tid, 0);
		idMap.put(tid, new HashSet<String>());
		return "0";
	}

	private String doQ3(String key) {
		StringBuilder sb = new StringBuilder();
		Statement stmt = null;
		key = key.substring(3);
		String[] keys = key.split(",");
		String id = keys[0];
		String start = keys[1];
		String end = keys[2];
		int num = Integer.parseInt(keys[3]);

		try {
			Class.forName("com.mysql.jdbc.Driver");
			stmt = conn.createStatement();
			String sql = "SELECT value from test3 where userid=\'" + id + "\' ";

			ResultSet rs = stmt.executeQuery(sql);

			String result = null;
			while (rs.next()) {
				result = rs.getString("value");
			}
			
			// has no posts for this id
			if (result == null) {
				result = "";
			}
			
			String[] posts = result.split("#&#");
//			for (String post:posts) {
//				System.out.println("post: " + post);
//			}
			int n = posts.length;
			// add positive
			if (n < num) {
				num = n;
			}
			sb.append("Positive Tweets;");
			for (int i=0; i<num; i++) {
				String post = posts[i];
				String[] ele = post.split(",");
				if (ele.length<2) {
					continue;
				}
				String time = ele[0];
				int score = Integer.parseInt(ele[1]);
				if (score > 0 && time.compareTo(start)>=0 && time.compareTo(end) <= 0) {
					sb.append(post + ";");
				} else if (score < 0) {
					break;
				}
			}
			
			// add negative
			sb.append(";Negative Tweets;");
			for (int i=0; i<num; i++) {
				String post = posts[n-i-1];
				String[] ele = post.split(",");
				if (ele.length < 2) {
					// text is not enough to store all the posts
					continue;
				}
				String time = ele[0];
				
				int score = Integer.parseInt(ele[1]);
				if (score < 0 && time.compareTo(start)>=0 && time.compareTo(end) <= 0) {
					sb.append(post + ";");
				} else if (score > 0) {
					break;
				}
			}
			stmt.close();
			rs.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	private String doQ4(String key) {
		StringBuilder sb = new StringBuilder();
		Statement stmt = null;
		key = key.substring(3);
		String[] keys = key.split(",");
		String hashTag = keys[1];
		String n = keys[0];
		try {
			Class.forName("com.mysql.jdbc.Driver");
			stmt = conn.createStatement();
			String sql = "SELECT * from test4 where hashTag=\'" + hashTag
					+ "\' ";

			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				String response = rs.getString("value");
				response = response.replace("$shit$", "\t");
				String[] posts = response.split("\t");

				int size = Integer.parseInt(n);
				int len = posts.length;

				for (int i = 0; i < size && i < len; i++) {
					String temp = posts[i];

					int left = 0;
					while (left != -1) {
						left = temp.indexOf("$fuck$");
						if (left != -1) {
							temp = temp.substring(0, left) + "\n"
									+ temp.substring(left + 6);
						}
					}
					sb.append(temp);
					sb.append(";");
				}
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	private String doQ2(String key) {
		// prepare to query
		StringBuilder sb = new StringBuilder();
		Statement stmt = null;
		key = key.substring(3);
		int mid = key.indexOf(",");
		String userid = key.substring(0, mid);
		String timestamp = key.substring(mid + 1);
		try {
			// build sql
			Class.forName("com.mysql.jdbc.Driver");
			stmt = conn.createStatement();

			String sql = "SELECT post from test2 where userid=" + userid
					+ " and newDate=\'" + timestamp + "\';";
			// query
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				sb.append(rs.getString("post"));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
	
}

class MyRequest implements Comparable<MyRequest> {
	String tweetid;
	int seq;
	String tag;
	String opt;

	public MyRequest(int seq, String tweetId, String tag, String opt) {
		this.seq = seq;
		this.tweetid = tweetId;
		this.tag = tag;
		this.opt = opt;
	}

	@Override
	public int compareTo(MyRequest o) {
		return seq - o.seq;
	}
}


