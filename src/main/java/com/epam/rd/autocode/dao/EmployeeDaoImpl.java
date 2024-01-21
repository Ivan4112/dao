package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class EmployeeDaoImpl implements EmployeeDao {
    private static final String INSERT = "INSERT INTO employee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String BY_ID = "SELECT * FROM employee WHERE id = ?";
    private static final String GET_ALL = "SELECT * FROM employee";
    private static final String UPDATE = "UPDATE employee SET firstName = ?, lastName = ?, middleName = ?, " +
            "position = ?, manager = ?, hiredate = ?, salary = ?, department = ?,  WHERE id = ?";
    private static final String DELETE = "DELETE FROM employee WHERE id = ?";
    private static final String GET_BY_DEP = "SELECT * FROM employee WHERE department = ?";
    private static final String GET_BY_MANAGER = "SELECT * FROM employee WHERE manager = ?";
    @Override
    public Optional<Employee> getById(BigInteger Id) {
        Optional<Employee> employeeOptional = Optional.empty();

        try(Connection connection = ConnectionSource.instance().createConnection();
            PreparedStatement statement = connection.prepareStatement(BY_ID)){
            statement.setInt(1, Id.intValue());

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String firstName = resultSet.getString("FIRSTNAME");
                    String lastName = resultSet.getString("LASTNAME");
                    String middleName = resultSet.getString("MIDDLENAME");
                    Position position = Position.valueOf(resultSet.getString("POSITION"));
                    LocalDate localDate = resultSet.getObject("HIREDATE", LocalDate.class);
                    BigDecimal salary = BigDecimal.valueOf(resultSet.getLong("SALARY"));
                    BigInteger manager = BigInteger.valueOf(resultSet.getLong("MANAGER"));
                    BigInteger department = BigInteger.valueOf(resultSet.getLong("DEPARTMENT"));

                    employeeOptional = employeeOptional.of(
                            new Employee(Id, new FullName(firstName, lastName, middleName),
                                    position, localDate, salary, manager, department));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeeOptional;
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employeeList = new ArrayList<>();

        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL)) {
            createEmployee(employeeList, statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeeList;
    }

    @Override
    public Employee save(Employee employee) {
        if (getById(employee.getId()).isPresent()) {
            try (Connection connection = ConnectionSource.instance().createConnection();
                 PreparedStatement statement = connection.prepareStatement(UPDATE)) {

                statement.setString(1, employee.getFullName().getFirstName());
                statement.setString(2, employee.getFullName().getLastName());
                statement.setString(3, employee.getFullName().getMiddleName());
                statement.setString(4, employee.getPosition().name());
                statement.setInt(5, employee.getManagerId().intValue());
                statement.setDate(6, Date.valueOf(employee.getHired()));
                statement.setBigDecimal(7, employee.getSalary());
                statement.setInt(8, employee.getDepartmentId().intValue());
                statement.setInt(9, employee.getId().intValue());
                statement.executeUpdate();

            }catch (SQLException e){
                e.printStackTrace();
            }
        }else {
            try (Connection connection = ConnectionSource.instance().createConnection();
                 PreparedStatement statement = connection.prepareStatement(INSERT)) {

                statement.setInt(1, employee.getId().intValue());
                statement.setString(2, employee.getFullName().getFirstName());
                statement.setString(3, employee.getFullName().getLastName());
                statement.setString(4, employee.getFullName().getMiddleName());
                statement.setString(5, employee.getPosition().name());
                statement.setInt(6, employee.getManagerId().intValue());
                statement.setDate(7, Date.valueOf(employee.getHired()));
                statement.setBigDecimal(8, employee.getSalary());
                statement.setInt(9, employee.getDepartmentId().intValue());
                statement.executeUpdate();

            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return employee;
    }

    @Override
    public void delete(Employee employee) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE)){
            statement.setInt(1, employee.getId().intValue());
            statement.executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> employeeList = new ArrayList<>();

        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_BY_DEP)) {
            statement.setInt(1, department.getId().intValue());
            createEmployee(employeeList, statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeeList;
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        List<Employee> employeeList = new ArrayList<>();

        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_BY_MANAGER)) {
            statement.setInt(1, employee.getId().intValue());
            createEmployee(employeeList, statement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeeList;
    }

    private void createEmployee(List<Employee> employeeList, PreparedStatement statement) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                BigInteger id = BigInteger.valueOf(resultSet.getLong("ID"));
                String firstName = resultSet.getString("FIRSTNAME");
                String lastName = resultSet.getString("LASTNAME");
                String middleName = resultSet.getString("MIDDLENAME");
                Position position = Position.valueOf(resultSet.getString("POSITION"));
                LocalDate localDate = resultSet.getObject("HIREDATE", LocalDate.class);
                BigDecimal salary = BigDecimal.valueOf(resultSet.getLong("SALARY"));
                BigInteger manager = BigInteger.valueOf(resultSet.getLong("MANAGER"));
                BigInteger depart = BigInteger.valueOf(resultSet.getLong("DEPARTMENT"));

                employeeList.add(new Employee(id, new FullName(firstName, lastName, middleName),
                        position, localDate, salary, manager, depart));
            }
        }
    }
}
