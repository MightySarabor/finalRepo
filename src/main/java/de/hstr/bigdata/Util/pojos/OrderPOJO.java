package de.hstr.bigdata.Util.pojos;

import de.hstr.bigdata.Util.Json.JSONSerdeCompatible;

import java.util.List;

public class OrderPOJO implements JSONSerdeCompatible {

    private String _t;
    private String customer;
    private List<PizzaPOJO> pizzas;


    public OrderPOJO(){

    }
    public OrderPOJO(String customer, List pizzas){
        _t = "order";
        this.customer = customer;
        this.pizzas = pizzas;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public List<PizzaPOJO> getPizzas() {
        return pizzas;
    }

    public void setPizzas(List<PizzaPOJO> pizzas) {
        this.pizzas = pizzas;
    }

    @Override
    public String toString() {
        return "OrderPOJO{" +
                "customer='" + customer + '\'' +
                ", pizzas=" + pizzas +
                '}';
    }
}
