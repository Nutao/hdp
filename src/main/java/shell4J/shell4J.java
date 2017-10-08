package shell4J;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class shell4J {
    public static void runScript(String command){
        try {
            Process ps = Runtime.getRuntime().exec(command);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String result = sb.toString();
            System.out.println(result);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
