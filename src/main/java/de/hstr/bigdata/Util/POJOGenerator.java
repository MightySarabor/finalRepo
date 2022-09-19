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
    /*public static String getRandomCustomer(String[] list)
    {
        int randomIndex = (int) (Math.random() * list.length);
        return list[randomIndex];
    }
    public final static List<String> generateCustomer(int number_of_customers){
        Faker faker = new Faker();
        List<String> names = new ArrayList<String>();
        for(int i = 0; i <=  number_of_customers; i++){
            String name = faker.name().fullName();
            String firstName = faker.name().firstName();
            String lastName = faker.name().lastName();
            names.add(name);
        }
        System.err.println("Liste erstellt");
        return names;
    }*/
    public static OrderPOJO generateOrder(String[] customers){

        List<PizzaPOJO> pizzas = new ArrayList<PizzaPOJO>();
        int numerOfOrders = ThreadLocalRandom.current().nextInt(0, 5);
        for(int i = 0; i <= numerOfOrders; i++)
        {
            pizzas.add(generatePizza());
        }
        System.err.println("Order erstellt");
        //return new OrderPOJO(getRandomCustomer(customers), pizzas);
        return null;
    }

    public static PizzaPOJO generatePizza(){
        String pizza = pizzas[ThreadLocalRandom.current().nextInt(0, pizzas.length)];
        int sizeAndPrice = ThreadLocalRandom.current().nextInt(0, 3);
        return new PizzaPOJO(pizza, sizes[sizeAndPrice], prices[sizeAndPrice]);
    }

    public static void main1(String[] args) throws JsonProcessingException {

        for(int i = 0; i <= 100000; i++) {
            //System.out.println(Json.prettyPrint(Json.toJson(generateOrder(500).getCustomer())));
            //generateOrder(500);
            //System.out.println(i);
            
        }
    }
}
