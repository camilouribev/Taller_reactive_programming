package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {
    private final String NATIONAL = "Colombia";
    private final String CLUB = "Cruzeiro";
    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }

    @Test
    void stream_filtrarJugadoresMayoresAPorClub(){
        List<Player> list = CsvUtilFile.getPlayers();
        System.out.println(CLUB.toUpperCase(Locale.ROOT));
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 34)
                .filter(player ->player.club.equals(CLUB))
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .distinct()
                .collect(Collectors.groupingBy(Player::getName));

            listFilter.forEach((s, players)->players.stream().forEach(player -> System.out.println(player.name+ " "+ player.age +" años")));

        assert listFilter.size()== 8;

    }
    @Test
    void reactive_filtrarJugadoresMayoresA34PorClub(){
        List<Player> list = CsvUtilFile.getPlayers();

        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        System.out.println(CLUB.toUpperCase(Locale.ROOT));
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 34)
                .filter(player -> player.club.equals(CLUB))
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    System.out.println(player.name+ " "+ player.age +" años");
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(player -> player.getClub());

        assert listFilter.block().size() == 1;
    }

    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }

    @Test
    void reactive_filtrarNacionalidad() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        System.out.println(NATIONAL.toUpperCase(Locale.ROOT));
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.getNational().equals(NATIONAL))
                .sort(Comparator.comparingInt(playerA -> -playerA.winners))
                .map(player -> {
                    player.setNational(player.national.toUpperCase(Locale.ROOT));
                    System.out.println( player.name+" "+player.winners );
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.national.equals(playerB.national)))
                )
                .distinct()

                .collectMultimap(Player::getNational);

        assert listFilter.block().size() == 1;
    }

}
