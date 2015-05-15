package Collect;

public class ExitTest {

    public static void main(String args[]) {

	unicycleWrapper wrapper = new unicycleWrapper(args) {

		public void opened() { }
		public void closed() { }
		public void reqComplete(int id) { }
		public void errorCode(int code) {

		    switch (code) {
		    case 502:
			// 502: Couldn't connect to TWS. Confirm that API is enabled in TWS via the Configure>API menu command.
			exit(1);
			break;
		    default:
			break;
		    }
		}
	    };
    }
}
