import java.io.File;
import java.io.PrintStream;

import redis.clients.jedis.Jedis;


public class PushToPhplist
{
	static final String SEP = "#sep#";
	
	public static void main(String[] args) 
	{
		PrintStream logOut = null;
		
		try {
			File logFile = new File("/home/ubuntu/ktemp/log.txt");
			if(!logFile.exists()) {
				logFile.createNewFile();
			}
			logOut = new PrintStream(logFile);
			
			Jedis jedis = new Jedis("localhost");
			
			while(true) {
				
				int keyNumber = 0;
				
				for(String key : jedis.keys("*")) {
					++keyNumber;
					String[] splitup = key.split("#sep#");
					if(splitup.length == 4) {
						String fromAddr = splitup[0];
						String toAddr = splitup[1];
						String subj = splitup[2];
						String content = splitup[3];
						// content = content.replaceAll("\"", "'");
						
						if(content == null || "".equals(content)) {
							logOut.println("content is null or empty. skipping this msg");
							jedis.del(key);
							continue;
						}
						
						logOut.println("working on key: " + keyNumber + " with toAddr: " + toAddr);
						
						String fileName = "/home/ubuntu/ktemp/phplist-3.0.12/bin/message.txt";
						File msgFile = new File(fileName);
						try {
							logOut.println("pushing the msg content to text file");

							if(!msgFile.exists()) {
								msgFile.createNewFile();
							}
							PrintStream msgStream = new PrintStream(msgFile);
							msgStream.print(content);
							msgStream.flush();
							msgStream.close();
						}
						catch(Exception ex) {
							ex.printStackTrace(logOut);
							jedis.del(key);
							continue;
						}
						
						try {
							String command = "/home/ubuntu/ktemp/phplist-3.0.12/bin/phplist  -psend -s " + subj + " -l " + toAddr + " -f " + fromAddr;
							Process p = Runtime.getRuntime().exec(command);
							int res = p.waitFor();
							logOut.println("command executed with return value: " + res);
							
							msgFile.delete();
							
							jedis.del(key);
							
							if(keyNumber % 5 == 0) {
								processQueue(logOut);	
							}
							
							if(keyNumber % 200 == 0) {
								// addDuplicateMailToAdmin
								String dup = fromAddr+SEP+"krishnasofts+spicecopy@gmail.com"+SEP+subj+SEP+content;
								jedis.set(dup, dup);
							}
						}
						catch(Exception ex) {
							ex.printStackTrace(logOut);
						}
					}
					else {
						logOut.println("skipping key: " + keyNumber + "bcz invalid splitup length:" + splitup.length);
						jedis.del(key);
					}
				}
				
				if(keyNumber != 0) {
					logOut.println("--" + keyNumber +"--");	
					processQueue(logOut);
				}
				
				try {
					/*
					 *  sleep for 15 second to avoid CPU spike
					 */
					Thread.sleep(1000*15);
				}
				catch(Exception e) {
					e.printStackTrace(logOut);
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			if(logOut != null) {
				logOut.flush();
				logOut.close();
			}
		}
	}
	
	private  static void processQueue(PrintStream logOut) throws Exception
	{
		logOut.println("processing the queue.");
		Process p = Runtime.getRuntime().exec("/home/ubuntu/ktemp/phplist-3.0.12/bin/phplist  -pprocessqueue");
		int res = p.waitFor();
		logOut.println("queue process command executed with return value: " + res);
	}
}

