package Collect;

public class CheckConnection {

    public static void main(String args[]) {

	unicycleWrapper wrapper = new unicycleWrapper() {
		public void opened() {
		    System.out.println("---------------- STATUS: OK ----------------");
		    exit(0);
		}
		public void closed() { }
		public void reqComplete(int code) { }
	    };
    }
}
