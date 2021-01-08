package fr.troisil.info.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
@Slf4j
@Data
@AllArgsConstructor
@Builder
public class Record implements Serializable {

    private String LibelleCommune;
    private String montant;



}
