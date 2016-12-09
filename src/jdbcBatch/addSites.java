package jdbcBatch;

import java.io.*;
import java.nio.Buffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dixonshen on 2016/12/7.
 */
public class addSites {
    private Connection conn = null;
    PreparedStatement statement = null;

    //connect to MySQL
    public void connSQL(){
        String url = "jdbc:mysql://localhost:3306/springtest";
        String username = "root";
        String password = "123456";

        //加载驱动程序以连接
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
            System.out.println("connect successfully!");
        }
        //捕获加载驱动程序异常
        catch (ClassNotFoundException cnfex){
            System.err.println("装载JDBC/ODBC 驱动程序失败。");
            cnfex.printStackTrace();
        }
        catch (SQLException sqlex){
            System.err.println("无法连接数据库");
            sqlex.printStackTrace();
        }
    }

    //disconnect to MySQL
    public void deconnSQL(){
        try {
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            System.out.println("关闭数据库问题：");
            e.printStackTrace();
        }

    }

    //execute selection language
    public ResultSet selectSQL(String sql){
        ResultSet resultSet = null;
        try {
            statement = conn.prepareStatement(sql);
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    //execute insertion language
    public boolean insertSQL(String sql){
        try {
            statement = conn.prepareStatement(sql);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("插入数据时出错：");
            e.printStackTrace();
        }
        return false;
    }

    //execute delete language
    public boolean deleteSQL(String sql){
        try {
            statement = conn.prepareStatement(sql);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("删除数据时出错：");
            e.printStackTrace();
        }
        return false;
    }

    //execute update language
    public boolean updateSQL(String sql){
        try {
            statement = conn.prepareStatement(sql);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("更新数据时出错：");
            e.printStackTrace();
        }
        return false;
    }

    //show data in pvsite
    public void showSites(ResultSet resultSet){
        System.out.println("------------------------");
        System.out.println("执行结果如下：");
        System.out.println("------------------------");
        System.out.println("SiteID" + "\t\t" + "SiteName");
        System.out.println("------------------------");
        try {
            while (resultSet.next()){
                System.out.println(resultSet.getInt("id") + "\t\t"
                        + resultSet.getString("PVSite"));
            }
        } catch (SQLException e) {
            System.out.println("显示数据库时出错。");
            e.printStackTrace();
        }
    }

    //show data in component
    public void showPanel(ResultSet resultSet){
        System.out.println("------------------------");
        System.out.println("执行结果如下：");
        System.out.println("------------------------");
        System.out.println("SiteID" + "\t\t" + "SiteName" + "\t\t" + "ArrayName" + "\t\t" + "PanelName");
        System.out.println("------------------------");
        try {
            while (resultSet.next()){
                System.out.println(resultSet.getInt("id") + "\t\t"
                        + resultSet.getString("PVSite") + "\t\t"
                        + resultSet.getString("PVArray") + "\t\t"
                        + resultSet.getString("CompoNo"));
            }
        } catch (SQLException e) {
            System.out.println("显示数据库时出错。");
            e.printStackTrace();
        }
    }

    public static List<String> readFile(String filePath) throws FileNotFoundException{
        List<String> files = new ArrayList<String>();
        try {
            File file = new File(filePath);
            if (file.isDirectory()) {
                String[] fileList = file.list();
                for (int i=0; i<fileList.length; i++){
                    File readfile = new File(filePath + "\\" + fileList[i]);
                    if (!readfile.isDirectory()){
                        files.add(readfile.getAbsolutePath());
                    }
                    else if (readfile.isDirectory()) {
                        files.addAll(readFile(readfile.getAbsolutePath()));
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("readFile() Exception: ");
            e.printStackTrace();
        }
        return files;
    }

    public static void main(String[] args) throws SQLException{
        addSites add = new addSites();
        add.connSQL();


//        String selectSites = "select * from pvsite";
//        String siteName = "S0";
//        String insertSite = "insert into pvsite(PVSite) values('" + siteName + "')";
//        if (add.insertSQL(insertSite) == true) {
//            System.out.println("insert successfully!");
//            ResultSet resultSet = add.selectSQL(selectSites);
//            add.showSites(resultSet);
//        }


//        String siteName = "S0";
//        for (int i = 1; i<=23; i++){
//            String arrayName = "S0_" + "Invpe" + i;
//            String insertArray = "insert into pvarray(PVArray,PVSite) values('"
//                    + arrayName + "','" + siteName + "')";
//            add.insertSQL(insertArray);
//            for (int j = 0; j<10; j++){
//                String panelNo = arrayName + "_P" + j;
//                String insertPanel = "insert into component(CompoNo,PVArray,PVSite) values('"
//                        + panelNo + "','" + arrayName + "','" + siteName + "')";
//                add.insertSQL(insertPanel);
//            }
//        }
//        String selectPanel = "select * from component";
//        add.showPanel(add.selectSQL(selectPanel));


//        String select1 = "select distinct PVArray from component";
//        ResultSet rs = add.selectSQL(select1);
//        try {
//            while (rs.next()){
//                System.out.println(rs.getString(1));
//            }
//        } catch (SQLException e){
//            System.out.println("显示错误");
//            e.printStackTrace();
//        }


//        String baseDir = "E:\\微电网实时数据平台\\后台数据监测网站\\panel数据";
//        BufferedReader br = null;
//        try {
//            int count = 0;
//            List<String> files = readFile(baseDir);
//            for (String file : files){
//                String datetime = "";
//                String power = "";
//                String energy = "";
//                String panelName = "";
//                System.out.println(file);
//                br = new BufferedReader(new FileReader(file));
//                String line = "";
//                int lineCount = 0;
//                String insert = "";
//                while ((line = br.readLine()) != null){
//                    count++;
//                    lineCount++;
//                    if (lineCount == 1) continue;
//                    System.out.println("第" + count + "条数据：" + line);
////                    String[] record = line.split(",");
////                    if (record.length != 4) {
////                        System.out.println("该条数据不符合要求，删去。");
////                        continue;
////                    }
////                    datetime = record[0];
////                    power = record[1];
////                    energy = record[2];
////                    panelName=record[3];
////                    insert = "insert into PanelState(time,power,energy,panelName) values('" +
////                            datetime + "','" + power + "','" + energy + "','" + panelName + "')";
////                    if (add.insertSQL(insert)){
////                        System.out.println("该条数据插入数据库成功！");
////                    } else System.out.println("数据插入失败！");
//                }
//            }
//            System.out.println(count);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        String baseDir = "E:\\panelData\\";

        int allCount = 0;
        int preCount = 1000;

        int iCount = 1;

        BufferedReader br;
        try {
            List<String> files = readFile(baseDir);
            for (String file : files){
                br = new BufferedReader(new FileReader(file));
                StringBuilder insert = new StringBuilder("insert into PanelState(time,power,energy,panelName) values ");
                String line = "";
                int lineCount = 0;
                long start = System.currentTimeMillis();
                while ((line = br.readLine()) != null){
                    allCount++;
                    lineCount++;
                    if (lineCount == 1) continue;
                    String[] record = line.split(",");
                    if (iCount > 1){
                        insert.append(",");
                    }
                    iCount++;
                    insert.append("('" +
                            record[0] + "','" + record[1] + "','" + record[2] + "','" + record[3] + "')");
                    if (((lineCount-1) % preCount) == 0) {

                    }
                }
                iCount = 1;
                add.insertSQL(insert.toString());
                System.out.println("文件" + file + "已插入" + (lineCount-1) + "条数据");
                System.out.println("已插入数据" + allCount + "条");
                long end = System.currentTimeMillis();
                System.out.println("文件" + file + "数据导入完毕， 所用时间为：" + (end - start) + "ms");
                add.conn.commit();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }



//        BufferedReader br = null;
//        try {
//            List<String> files = readFile(baseDir);
//            for (String file : files){
//                n
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        try {
//            List<String> files = readFile(baseDir);
//            for (String file : files){
//                long start = System.currentTimeMillis();
//                String[] paras = file.split("\\\\");
//                System.out.println(paras[0]);
//                System.out.println(file);
//                String sql = "LOAD DATA LOCAL INFILE \"" + file
//                        + "\" INTO TABLE PanelState FIELDS TERMINATED BY ','";
//                System.out.println(sql);
//                add.insertSQL(sql);
//                long end = System.currentTimeMillis();
//                System.out.println(file + "数据导入完毕， 所用时间为：" + (end - start) + "ms");
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }

        add.deconnSQL();
    }
}
