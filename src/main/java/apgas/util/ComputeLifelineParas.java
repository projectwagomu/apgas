package apgas.util;

public class ComputeLifelineParas {

  public static int computeZ(int l, int numPlaces) {
    int z0 = 1;
    int zz = l;
    while (zz < numPlaces) {
      z0++;
      zz *= l;
    }
    return z0;
  }

  public static int computeL(int numPlaces) {
    int l = 1;
    while (Math.pow(l, l) < numPlaces) {
      l++;
    }
    return l;
  }
}
