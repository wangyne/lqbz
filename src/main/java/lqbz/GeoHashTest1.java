package lqbz;

import ch.hsr.geohash.GeoHash;

public class GeoHashTest1 {
    public static void main(String[] args) {

        GeoHash geoHash=GeoHash.withBitPrecision(114.323857,30.555892,5);

        System.out.println(geoHash.toBinaryString());
    }
}