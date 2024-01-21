package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Position;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDaoImpl implements DepartmentDao {
    private static final String INSERT = "INSERT INTO department VALUES(?, ?, ?)";
    private static final String BY_ID = "SELECT * FROM department WHERE id = ?";
    private static final String GET_ALL = "SELECT * FROM department";
    private static final String UPDATE = "UPDATE department SET name = ?, location = ? WHERE id = ?";
    private static final String DELETE = "DELETE FROM department WHERE id = ?";
    @Override
    public Optional<Department> getById(BigInteger Id) {
        Optional<Department> departments = Optional.empty();

        try(Connection connection = ConnectionSource.instance().createConnection();
            PreparedStatement statement = connection.prepareStatement(BY_ID)){
            statement.setInt(1, Id.intValue());

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString("NAME");
                    String location = resultSet.getString("LOCATION");
                    departments = departments.of(new Department(Id, name, location));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return departments;
    }

    @Override
    public List<Department> getAll() {
        List<Department> departmentList = new ArrayList<>();

        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(GET_ALL)) {

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    BigInteger id = BigInteger.valueOf(resultSet.getLong("ID"));
                    String name = resultSet.getString("NAME");
                    String location = resultSet.getString("LOCATION");
                    departmentList.add(new Department(id, name, location));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return departmentList;
    }

    @Override
    public Department save(Department department) {
        if (getById(department.getId()).isPresent()) {
            try (Connection connection = ConnectionSource.instance().createConnection();
                 PreparedStatement statement = connection.prepareStatement(UPDATE)) {

                statement.setString(1, department.getName());
                statement.setString(2, department.getLocation());
                statement.setInt(3, department.getId().intValue());

                statement.execute();

            }catch (SQLException e){
                e.printStackTrace();
            }
        }else {
            try (Connection connection = ConnectionSource.instance().createConnection();
                 PreparedStatement statement = connection.prepareStatement(INSERT)) {

                statement.setInt(1, department.getId().intValue());
                statement.setString(2, department.getName());
                statement.setString(3, department.getLocation());
                statement.executeUpdate();

            }catch (SQLException e){
                e.printStackTrace();
            }
        }
        return department;
    }

    @Override
    public void delete(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE)){
            statement.setInt(1, department.getId().intValue());
            statement.executeUpdate();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
