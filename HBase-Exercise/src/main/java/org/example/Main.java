package org.example;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileDescriptor;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;

/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 9/9/24
 **/
public class Main {

    private Connection connection;
    private Admin admin;
    private TableName table = TableName.valueOf("Users");
    private static final String[] NAMES = {"Juan", "Luis", "Carlos", "Ana", "María", "Pedro", "Sofía", "Carmen", "Javier", "Laura"};
    private static final String[] LAST_NAMES = {"García", "Martínez", "López", "Sánchez", "Fernández", "González", "Pérez", "Rodríguez", "Gómez", "Ruiz"};
    private static final String[] PROVINCES = {"Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Málaga", "Murcia", "Palma", "Bilbao", "Valladolid"};
    private static final String[] STREETS = {"Calle Mayor", "Gran Vía", "Avenida del Sol", "Paseo de la Castellana", "Calle de Alcalá", "Rambla de Cataluña"};


    private byte[] generateKey(String lastName,String name,int id) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(lastName),0,key,0,lastName.length());
        System.arraycopy(Bytes.toBytes(name),0,key,20,name.length());
        System.arraycopy(Bytes.toBytes(id),0,key,40,4);

        return key;
    }

    private byte[] generateStartKey(String lastName) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(lastName),0,key,0,lastName.length());
        for (int i = 20; i < 44; i++){
            key[i] = (byte)-255;
        }
        return key;
    }

    private byte[] generateEndKey(String lastName) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(lastName),0,key,0,lastName.length());
        for (int i = 20; i < 44; i++){
            key[i] = (byte)255;
        }
        return key;
    }

    private void createTable() throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        HColumnDescriptor family1 = new HColumnDescriptor(Bytes.toBytes("UserInfo"));
        HColumnDescriptor family2 = new HColumnDescriptor(Bytes.toBytes("Session"));
        family2.setMaxVersions(10); // Default is 3.

        admin.createTable(new HTableDescriptor(table)
                .addFamily(family1)
                .addFamily(family2)
        );
        admin.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void loadUsers(int numberUsers) throws IOException {
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        for (int i = 1; i <= numberUsers; i++) {
            // Generate random data for each user
            String name = getRandom(NAMES);
            String lastName = getRandom(LAST_NAMES);
            String province = getRandom(PROVINCES);
            String address = getRandom(STREETS) + ", " + (new Random().nextInt(100) + 1);
            String phoneNumber = "+34 " + (600000000 + new Random().nextInt(100000000));
            String lastLogin = generateRandomDate();

            // Crear la fila con un ID único
            byte[] rowKey = generateKey(lastName, name, i);
            Put put = new Put(rowKey);

            // Insert data in 'UserInfo' family
            put.addColumn(Bytes.toBytes("UserInfo"), Bytes.toBytes("name"), Bytes.toBytes(name));
            put.addColumn(Bytes.toBytes("UserInfo"), Bytes.toBytes("lastName"), Bytes.toBytes(lastName));
            put.addColumn(Bytes.toBytes("UserInfo"), Bytes.toBytes("phoneNumber"), Bytes.toBytes(phoneNumber));
            put.addColumn(Bytes.toBytes("UserInfo"), Bytes.toBytes("province"), Bytes.toBytes(province));
            put.addColumn(Bytes.toBytes("UserInfo"), Bytes.toBytes("address"), Bytes.toBytes(address));

            // Insert el first login in 'Session' family
            put.addColumn(Bytes.toBytes("Session"), Bytes.toBytes("lastLogin"), Bytes.toBytes(lastLogin));

            // Insert row in table
            t.put(put);

            int numberLastLogin=new Random().nextInt(10)+1;
            for (int s=0; s<numberLastLogin; s++) {
                lastLogin = generateRandomDate();
                put.addColumn(Bytes.toBytes("Session"), Bytes.toBytes("lastLogin"), Bytes.toBytes(lastLogin));
                t.put(put);
            }

            // Imprimir el usuario insertado (opcional)
            System.out.println("User: " + name + " " + lastName + " with id " + i);
        }
        t.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    // Auxiliary method to get a random element from an array
    private static String getRandom(String[] array) {
        Random random = new Random();
        return array[random.nextInt(array.length)];
    }

    // Method to generate a random date between 2010 and 2023
    private static String generateRandomDate() {
        Random random = new Random();
        int year = 2010 + random.nextInt(14); // Entre 2010 y 2023
        int month = 1 + random.nextInt(12);   // Entre 1 y 12
        int day = 1 + random.nextInt(28);     // Entre 1 y 28 para simplificar

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(year - 1900, month - 1, day));
    }

    /**
     * Check if user with UserName “Romeo” and LastName “Rodriguez” with Id = 1 exists
     */
    private void query0(String lastName,String name,int id) throws IOException {
        System.out.println("Check if user with UserName "+name+" and LastName "+lastName+" with Id = "+id+" exists");
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        byte[] key = generateKey(lastName,name,id);
        Get get = new Get(key);
        Result res = t.get(get);
        if(res != null && !res.isEmpty()){
            System.out.println("The user " + name + " " + lastName + " with id " + id+" exists");
        }
        else{
            System.out.println("The user " + name + " " + lastName + " with id " + id+" doesn't exist");
        }


        t.close();
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * Count all the Users
     */
    private void query1() throws IOException {
        System.out.println("Count all the users");
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        Scan scan = new Scan();
        ResultScanner rs = t.getScanner(scan);
        Result res = rs.next();
        int nRows = 0;
        while (res!=null && !res.isEmpty()){
            nRows++;
            res = rs.next();
        }

        System.out.println("Number of users: "+nRows);
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * Query all the Users with LastName = “Rodriguez”
     */
    private void query2(String lastName) throws IOException {
        System.out.println("Query all the Users with LastName = "+ lastName);
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        Scan scan = new Scan(generateStartKey(lastName),generateEndKey(lastName));
        ResultScanner rs = t.getScanner(scan);
        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            String name = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("name")));
            lastName = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("lastName")));
            String province = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("province")));
            String phone = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("phoneNumber")));
            String address = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("address")));
            String session = Bytes.toString(res.getValue(Bytes.toBytes("Session"),Bytes.toBytes("lastLogin")));

            System.out.println("Name: "+name+" Last name: "+lastName+" province: "+province+" phone: "+phone+" address: "+address+" Last session: "+session);
            res = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * Query all the Users with LastName = “Rodriguez” from Province = Soria
     */
    private void query3(String lastName, String province) throws IOException {
        System.out.println("Query all the Users with LastName = "+ lastName +" from Province = "+ province);
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        Scan scan = new Scan(generateStartKey(lastName),generateEndKey(lastName));

        Filter f = new SingleColumnValueFilter(Bytes.toBytes("UserInfo"), Bytes.toBytes("province"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes(province));
        scan.setFilter(f);
        ResultScanner rs = t.getScanner(scan);
        Result res = rs.next();
        while (res!=null && !res.isEmpty()){
            String name = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("name")));
            lastName = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("lastName")));
            province = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("province")));
            String phone = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("phoneNumber")));
            String address = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("address")));
            String session = Bytes.toString(res.getValue(Bytes.toBytes("Session"),Bytes.toBytes("lastLogin")));

            System.out.println("Name: "+name+" Last name: "+lastName+" province: "+province+" phone: "+phone+" address: "+address+" Last session: "+session);
            res = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * List the last 3 sessions from a given user
     */
    private void query4(String lastName, String name, int id) throws IOException {
        System.out.println("List the last 3 sessions from user "+ name +" "+ lastName +" with id "+id);
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        byte[] key = generateKey(lastName,name,id);
        Get get = new Get(key);
        get.setMaxVersions(3);
        get.addColumn(Bytes.toBytes("Session"),Bytes.toBytes("lastLogin"));
        Result result = t.get(get);
        if (result != null && !result.isEmpty()){
            CellScanner scanner = result.cellScanner();
            while (scanner.advance()) {
                Cell cell = scanner.current();
                byte[] value = CellUtil.cloneValue(cell);
                System.out.println(Bytes.toString(value));
            }
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * Count all the Users using a Coprocessor
     */
    private void query5() throws IOException {
        System.out.println("Count all the Users using a Coprocessor");
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.newBuilder().build();
        try {
            Map<byte[], Long> results = t.coprocessorService(
                    ExampleProtos.RowCountService.class,
                    null, /* start key */
                    null, /* end key */
                    new Batch.Call<ExampleProtos.RowCountService, Long>() {
                        @Override
                        public Long call(ExampleProtos.RowCountService aggregate) throws IOException {
                            CoprocessorRpcUtils.BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback = new
                                    CoprocessorRpcUtils.BlockingRpcCallback<>();
                            aggregate.getRowCount(null, request, rpcCallback);
                            ExampleProtos.CountResponse response = rpcCallback.get();
                            return response.hasCount() ? response.getCount() : 0L;
                        }
                    }
            );
            for (Long sum : results.values()) {
                System.out.println("Number of users = " + sum);
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    /**
     * Count all the Users
     */
    private void query6() throws IOException {
        System.out.println("All users name, last name, id and province");
        long start = System.currentTimeMillis();
        connection = ConnectionFactory.createConnection();
        Table t = connection.getTable(table);
        Scan scan = new Scan();
        ResultScanner rs = t.getScanner(scan);
        Result res = rs.next();

        while (res!=null && !res.isEmpty()){
            byte[] key = res.getRow();
            int id = Bytes.toInt(Arrays.copyOfRange(key, key.length - 4, key.length));
            String name = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("name")));
            String lastName = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("lastName")));
            String province = Bytes.toString(res.getValue(Bytes.toBytes("UserInfo"),Bytes.toBytes("province")));

            System.out.println("Name: "+name+" Last name: "+lastName+" id: "+id+" province: "+province);
            res = rs.next();
        }
        connection.close();
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        System.out.println("Time elapsed: "+timeElapsed+"ms.");
    }

    private void deleteAll() throws IOException {
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
        admin.disableTable(table);
        admin.deleteTable(table);
        admin.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {

        Scanner scanner = new Scanner(System.in);
        int input = 0;
        System.out.println("Welcome to the HBase exercise propose during the Cloud Computing and Big Data Ecosystems course.");
        System.out.println("Next, all possible actions are listed, write the number and press enter to execute the desired action:");
        System.out.println("  1: Create the table Users");
        System.out.println("  2: Load data");
        System.out.println("  3: Run query 0-> Check if user with UserName “Romeo” and LastName “Rodriguez” with Id = 1 exists");
        System.out.println("  4: Run query 1-> Count all the Users");
        System.out.println("  5: Run query 2-> Query all the Users with LastName = “Rodriguez”");
        System.out.println("  6: Run query 3-> Query all the Users with LastName = “Rodriguez” from Province = Soria");
        System.out.println("  7: Run query 4-> List the last 3 sessions from a given user");
        System.out.println("  8: Run coprocessor query -> Count all the Users");
        System.out.println("  9: Run query -> Get all users name, lastname, id and province");
        System.out.println("  -1: Exit and remove table");
        System.out.println("  -2: Remember actions");

        Main main = new Main();
        String lastname = "";
        String name = "";
        int id=-1;

        // Loop while input different to -1
        while (input != -1) {
            // Read user input
            input = scanner.nextInt();

            // If input is not -1
            if (input != -1) {
                System.out.println("Your input: " + input);
                switch (input){
                    case 1:
                        main.createTable();
                        System.out.println("Table created");
                        break;
                    case 2:
                        System.out.println("Enter the number of users to create");
                        int numUsers = scanner.nextInt();
                        main.loadUsers(numUsers);
                        System.out.println("Users loaded");
                        break;
                    case 3:
                        System.out.println("Enter lastname: ");
                        lastname = scanner.next();
                        System.out.println("Enter name");
                        name = scanner.next();
                        System.out.println("Enter user id");
                        id = scanner.nextInt();
                        main.query0(lastname, name, id);
                        break;
                    case 4:
                        main.query1();
                        break;
                    case 5:
                        System.out.println("Enter lastname: ");
                        lastname = scanner.next();
                        main.query2(lastname);
                        break;
                    case 6:
                        System.out.println("Enter lastname: ");
                        lastname = scanner.next();
                        System.out.println("Enter province: ");
                        String province = scanner.next();
                        main.query3(lastname, province);
                        break;
                    case 7:
                        System.out.println("Enter lastname: ");
                        lastname = scanner.next();
                        System.out.println("Enter name");
                        name = scanner.next();
                        System.out.println("Enter user id");
                        id = scanner.nextInt();
                        main.query4(lastname, name, id);
                        break;
                    case 8:
                        main.query5();
                        break;
                    case 9:
                        main.query6();
                        break;
                    case -2:
                        System.out.println("Next, all possible actions are listed, write the number and press enter to execute the desired action:");
                        System.out.println("  1: Create the table Users");
                        System.out.println("  2: Load data");
                        System.out.println("  3: Run query 0-> Check if user with UserName “Romeo” and LastName “Rodriguez” with Id = 1 exists");
                        System.out.println("  4: Run query 1-> Count all the Users");
                        System.out.println("  5: Run query 2-> Query all the Users with LastName = “Rodriguez”");
                        System.out.println("  6: Run query 3-> Query all the Users with LastName = “Rodriguez” from Province = Soria");
                        System.out.println("  7: Run query 4-> List the last 3 sessions from a given user");
                        System.out.println("  8: Run coprocessor query -> Count all the Users");
                        System.out.println("  9: Run query -> Get all users name, lastname, id and province");
                        System.out.println("  -1: Exit and remove table");
                        System.out.println("  -2: Remember actions");
                        break;
                }

            } else {
                System.out.println("Ending the program...");
                System.out.println("Do you want to delete the Users table? (Y/N)");
                if (scanner.next().equals("Y")) {
                    main.deleteAll();
                    System.out.println("Table deleted");
                }
            }
        }

        // Cerrar el objeto Scanner
        scanner.close();

    }
}

