package Collect;

import org.netbeans.jemmy.ClassReference;
import org.netbeans.jemmy.operators.JFrameOperator;
import org.netbeans.jemmy.operators.JTextFieldOperator;
import org.netbeans.jemmy.operators.JPasswordFieldOperator;
import org.netbeans.jemmy.operators.JButtonOperator;

public class AutoLogin {

    private String arg_tws_dir;
    private String arg_ib_url;
    private String arg_username;
    private String arg_password;

    public void parseArgs(String args[]) {
	int ii = 0;
	while (ii < args.length) {
	    String arg = args[ii];
	    if (arg.equals("-tws_dir")) {
		arg_tws_dir = args[++ii];
	    } else if (arg.equals("-ib_url")) {
		arg_ib_url = args[++ii];
	    } else if (arg.equals("-u") || arg.equals("-username")) {
		arg_username = args[++ii];
	    } else if (arg.equals("-p") || arg.equals("-password")) {
		arg_password = args[++ii];
	    }
	    ii++;
	}
    }

    public AutoLogin (String args[]) {

	parseArgs(args);

        try {
 	    String [] params = {arg_tws_dir, arg_ib_url};
  	    new ClassReference("jclient.LoginFrame").startApplication(params);

 	    JFrameOperator loginFrame = new JFrameOperator("Login");
 	    JTextFieldOperator userNameField = new JTextFieldOperator(loginFrame);
 	    JPasswordFieldOperator passwordField = new JPasswordFieldOperator(loginFrame);
 	    JButtonOperator loginButton = new JButtonOperator(loginFrame, "Login");

 	    loginFrame.requestFocus();

 	    userNameField.requestFocus();
 	    userNameField.typeText(arg_username);

 	    passwordField.requestFocus();
 	    passwordField.typeText(arg_password);

 	    loginButton.requestFocus();
 	    loginButton.push();

	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static void main(String[] argv) {
	new AutoLogin(argv);
    }
}