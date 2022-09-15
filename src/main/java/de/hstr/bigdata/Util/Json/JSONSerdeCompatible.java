package de.hstr.bigdata.Util.Json;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderPOJO.class, name = "order"),
        @JsonSubTypes.Type(value = PizzaPOJO.class, name = "pizza")})

public interface JSONSerdeCompatible {

}