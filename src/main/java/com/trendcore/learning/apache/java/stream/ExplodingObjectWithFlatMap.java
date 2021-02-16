package com.trendcore.learning.apache.java.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExplodingObjectWithFlatMap {

    static class Employee {

        String id;

        String firstname;

        String lastName;

        List list = new ArrayList();

        public Employee() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getFirstname() {
            return firstname;
        }

        public void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        public List getList() {
            return list;
        }

        public void setList(List list) {
            this.list = list;
        }
    }

    static class EmployeeExtendedObject {
        String id;

        String firstname;

        String lastName;

        Object e;

        public EmployeeExtendedObject(String id, String firstname, String lastName, Object e) {
            this.id = id;
            this.firstname = firstname;
            this.lastName = lastName;
            this.e = e;
        }
    }

    public static void main(String[] args) {

        Employee employee1 = getEmployee("Adam", "Stark", "1", Arrays.asList(1, 2, 3));
        Employee employee2 = getEmployee("Ann", "Bricks", "2", Arrays.asList(1, 2, 3, 4, 5, 6));

        Stream.of(employee1, employee2).flatMap(employee -> {
            List<Object> list = employee.getList();

            Stream<EmployeeExtendedObject> employeeExtendedObjectStream = list
                    .stream()
                    .map(o -> new EmployeeExtendedObject(employee.id, employee.firstname, employee.lastName, o));

            return employeeExtendedObjectStream;
        }).forEach(employeeExtendedObject -> {
            System.out.println(employeeExtendedObject.id + " " + employeeExtendedObject.firstname + " " + employeeExtendedObject.lastName + " " + employeeExtendedObject.e);
        });

    }

    private static Employee getEmployee(String firstname, String lastname, String id, List<Integer> integers1) {
        Employee emp1 = new Employee();
        emp1.firstname = firstname;
        emp1.lastName = lastname;
        emp1.id = id;

        emp1.list.addAll(integers1);
        return emp1;
    }

}
