package jdbcBatch;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dixonshen on 2016/12/9.
 */
public class csvFormat {

    public boolean exportCsv(File file, List<String> dataList){
        boolean isSuccess = false;

        FileOutputStream out = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;
        try {
            out = new FileOutputStream(file);
            osw = new OutputStreamWriter(out, "UTF-8");
            bw = new BufferedWriter(osw);
            if (dataList != null && !dataList.isEmpty()){
                for (String data : dataList){
                    bw.append(data).append("\r");
                }
            }
            isSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bw != null){
                try {
                    bw.close();
                    osw.close();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return isSuccess;
    }

    public List<String> importCsv(File file){
        List<String> dataList = new ArrayList<String>();

        int count = 0;

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line = "";
            while ((line = br.readLine()) != null){
                count++;
                if (count == 1) continue;
                dataList.add(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null){
                try {
                    br.close();
                    br = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return dataList;
    }
}
