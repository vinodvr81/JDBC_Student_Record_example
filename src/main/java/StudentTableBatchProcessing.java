
import java.sql.*;

public class StudentTableBatchProcessing {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/edureka";
    private static final String DB_USER = "pyspark";
    private static final String DB_PASSWORD = "p";

    public static void main(String[] args) {
        Connection connection = null;
        try {
            // Step 1: Establish the database connection
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false); // Disable auto-commit

            // Step 2: Create the student table
            createStudentTable(connection);

            // Step 3: Perform batch insert of student records
            batchInsertStudentRecords(connection);

            // Step 4: Commit the batch operation
            connection.commit();
            System.out.println("Batch processing completed successfully.");

            // Step 5: Select and display all student records
            selectAllStudents(connection);
        } catch (SQLException e) {
            e.printStackTrace();
            // Rollback the transaction if an error occurs
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            // Step 6: Clean up resources
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createStudentTable(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS student (" +
                "student_id VARCHAR(10) PRIMARY KEY," +
                "name VARCHAR(100)," +
                "class VARCHAR(50)," +
                "marks INT" +
                ")";
        Statement statement = connection.createStatement();
        statement.execute(createTableSQL);
        System.out.println("Student table created successfully.");
        statement.close();
    }

    private static void batchInsertStudentRecords(Connection connection) throws SQLException {
        String insertRecordSQL = "INSERT INTO student (student_id, name, class, marks) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertRecordSQL);

        // Add multiple records to the batch
        addRecordToBatch(preparedStatement, "S003", "Varun Vukkalam", "Class 1", 90);
        addRecordToBatch(preparedStatement, "S004", "Vishnu Vukkalam", "Class 5", 60);
        

        // Execute the batch operation
        preparedStatement.executeBatch();
        System.out.println("Batch insert completed.");
        preparedStatement.close();
    }

    private static void addRecordToBatch(PreparedStatement preparedStatement, String studentId, String name, String className, int marks) throws SQLException {
        preparedStatement.setString(1, studentId);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, className);
        preparedStatement.setInt(4, marks);
        preparedStatement.addBatch();
    }

    private static void selectAllStudents(Connection connection) throws SQLException {
        String selectRecordsSQL = "SELECT * FROM student";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(selectRecordsSQL);

        System.out.println("Student records:");
        while (resultSet.next()) {
            String studentId = resultSet.getString("student_id");
            String name = resultSet.getString("name");
            String className = resultSet.getString("class");
            int marks = resultSet.getInt("marks");
            System.out.println("Student ID: " + studentId +
                    ", Name: " + name +
                    ", Class: " + className +
                    ", Marks: " + marks);
        }
        resultSet.close();
        statement.close();
    }
}
