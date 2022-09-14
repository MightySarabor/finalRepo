package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.javafaker.Faker;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class POJOGenerator {
    final static private String[] pizzas = {"Hawaii", "Chicago-Style Deep Dish", "Caprese", "Quattro Formaggi",
            "New York Style", "Marinara", "Pepperoni", "Calzone", "Margherita","Napoletana"};
    final static private String[] sizes ={"S", "M", "L"};
    final static private float[] prices = {6.5F, 9.5F, 12.5F};

    public static OrderPOJO generateOrder(){
        Faker faker = new Faker();

        String name = faker.name().fullName();
        String firstName = faker.name().firstName();
        String lastName = faker.name().lastName();

        List<PizzaPOJO> pizzas = new ArrayList<PizzaPOJO>();
        int numerOfOrders = ThreadLocalRandom.current().nextInt(0, 5);
        for(int i = 0; i <= numerOfOrders; i++)
        {
            pizzas.add(generatePizza());
        }
        return new OrderPOJO(name, pizzas);
    }

    public static PizzaPOJO generatePizza(){
        String pizza = pizzas[ThreadLocalRandom.current().nextInt(0, pizzas.length)];
        int sizeAndPrice = ThreadLocalRandom.current().nextInt(0, 3);
        return new PizzaPOJO(pizza, sizes[sizeAndPrice], prices[sizeAndPrice]);
    }

    public static void main(String[] args) throws JsonProcessingException {

        for(int i = 0; i <= 100000; i++) {
            //System.out.println(Json.prettyPrint(Json.toJson(generateOrder())));
            generateOrder();
            System.out.println(i);
        }
    }
}
