import java.sql.*;

public class StudentTableOperations {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/edureka";
    private static final String DB_USER = "pyspark";
    private static final String DB_PASSWORD = "p";

    public static void main(String[] args) {
        try {
            // Step 1: Establish the database connection
            Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);

            // Step 2: Create the student table
            createStudentTable(connection);

            // Step 3: Insert records into the student table
            insertStudentRecord(connection, "S001", "Vinod Vukkalam", "Class 18", 85);
            insertStudentRecord(connection, "S002", "Nevaan Vukkalam", "Class 1", 90);
            insertStudentRecord(connection, "S003", "Lakshmi Vukkalam", "Class 5", 60);

            // Step 4: Update a student record
            updateStudentRecord(connection, "S001", "Vinod Vukkalam", "Class 18", 95);

            // Step 5: Delete a student record
            deleteStudentRecord(connection, "S003");

            // Step 6: Select and display all student records
            selectAllStudents(connection);

            // Step 7: Clean up resources
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
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

    private static void insertStudentRecord(Connection connection, String studentId, String name, String className, int marks) throws SQLException {
        String insertRecordSQL = "INSERT INTO student (student_id, name, class, marks) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertRecordSQL);
        preparedStatement.setString(1, studentId);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, className);
        preparedStatement.setInt(4, marks);
        preparedStatement.executeUpdate();
        System.out.println("Record inserted successfully.");
        preparedStatement.close();
    }

    private static void updateStudentRecord(Connection connection, String i, String name, String className, int marks) throws SQLException {
        String updateRecordSQL = "UPDATE student SET name = ?, class = ?, marks = ? WHERE student_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(updateRecordSQL);
        preparedStatement.setString(1, name);
        preparedStatement.setString(2, className);
        preparedStatement.setInt(3, marks);
        preparedStatement.setString(4, i);
        preparedStatement.executeUpdate();
        System.out.println("Record updated successfully.");
        preparedStatement.close();
    }

    private static void deleteStudentRecord(Connection connection, String studentId) throws SQLException {
        String deleteRecordSQL = "DELETE FROM student WHERE student_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(deleteRecordSQL);
        preparedStatement.setString(1, studentId);
        preparedStatement.executeUpdate();
        System.out.println("Record deleted successfully.");
        preparedStatement.close();
    }

    private static void selectAllStudents(Connection connection) throws SQLException {
        String selectRecordsSQL = "SELECT * FROM student";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(selectRecordsSQL);
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