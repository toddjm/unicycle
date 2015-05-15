package Collect;

public class Asset {

    public static void main(String args[]) {

	unicycleWrapper w = new unicycleWrapper(args) {

		public void opened() {
		    // Can not wait for a 2106; request itself establishes a connection with historical farm
  		    reqHistoricalData();
		}

		public void closed() { }

		public void reqComplete(int id) {
		    if (id == this.getInt(getVar("TickerId"))) {
			message(String.format("[reqHistoricalData:%d] Complete.", id));
			exit(0);
		    }
		}
	    };
    }
}
