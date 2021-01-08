package fr.troisil.info.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class dataSample implements Serializable {
    private String key;
    private Double MONTANT_TOTAL;
    private String LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE;
    private String NOM_PRENOM_OU_RAISON_SOCIALE;


    public String toKafkaMessage(){
        return String.format("%s_%s_%s_%s",key, MONTANT_TOTAL, LIBELLE_DE_LA_COMMUNE_DE_RESIDENCE, NOM_PRENOM_OU_RAISON_SOCIALE);
    }
}
