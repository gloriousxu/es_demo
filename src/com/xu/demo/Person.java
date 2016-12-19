package com.xu.demo;

public class Person {
    private int age;
    private String name;
    public void say(){
        System.out.println("Hello");
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }
}
