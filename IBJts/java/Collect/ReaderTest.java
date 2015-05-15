package Collect;

public class ReaderTest {

    public static void main(String args[]) {

	unicycleWrapper wrapper = new unicycleWrapper(args) {

		public void opened() { }
		public void closed() { }
		public void reqComplete(int id) { }
		public void errorCode(int code) { }
	    };
    }
}
