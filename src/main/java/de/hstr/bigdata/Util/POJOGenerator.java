package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import de.hstr.bigdata.Util.Json.Json;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class POJOGenerator {
    final static private String[] pizzas = {"Hawaii", "Chicago-Style Deep Dish", "Caprese", "Quattro Formaggi",
            "New York Style", "Marinara", "Pepperoni", "Calzone", "Margherita","Napoletana"};
    final static private String[] sizes ={"S", "M", "L"};
    final static private float[] prices = {6.5F, 9.5F, 12.5F};

    // Function select an element base on index
    // and return an element
    public static String getRandomCustomer(String[] names)
    {
        int randomIndex = (int) (Math.random() * names.length);
        return names[randomIndex];
    }
    public final static String[] generateCustomer(int number_of_customers){
        Faker faker = new Faker();
        String[] names = new String[number_of_customers];
        for(int i = 0; i <  number_of_customers; i++){
            String name = faker.name().fullName();
            //String firstName = faker.name().firstName();
            //String lastName = faker.name().lastName();
            names[i] = name;
        }
        return names;
    }
    public static OrderPOJO generateOrder(String[] customers){

        List<PizzaPOJO> pizzas = new ArrayList<PizzaPOJO>();
        int numerOfOrders = ThreadLocalRandom.current().nextInt(0, 5);
        for(int i = 0; i < numerOfOrders; i++)
        {
            pizzas.add(generatePizza());
        }
        //System.err.println("Order erstellt");
        return new OrderPOJO(getRandomCustomer(customers), pizzas);
    }

    public static PizzaPOJO generatePizza(){
        String pizza = pizzas[ThreadLocalRandom.current().nextInt(0, pizzas.length)];
        int sizeAndPrice = ThreadLocalRandom.current().nextInt(0, 3);
        return new PizzaPOJO(pizza, sizes[sizeAndPrice], prices[sizeAndPrice]);
    }

    public static void main(String[] args) throws JsonProcessingException {

        String[] names = generateCustomer(5);
        for(int i = 0; i < 5; i++){
            System.err.println(names[i]);
            
        }
    }
}
