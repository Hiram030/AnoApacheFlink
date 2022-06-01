package table;

import org.apache.flink.api.java.tuple.Tuple6;

public class Person {
    private int id;
    private String gender;
    private int age;
    private String name;
    private String surname;
    private String residence;

    public Person(Tuple6<Integer, String, Integer, String, String, String> tuple) {

        this.id = tuple.f0;
        this.gender = tuple.f1;
        this.age = tuple.f2;
        this.name = tuple.f3;
        this.surname = tuple.f4;
        this.residence = tuple.f5;

    }
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getResidence() {
        return residence;
    }

    public void setResidence(String residence) {
        this.residence = residence;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", residence='" + residence + '\'' +
                '}';
    }
}
