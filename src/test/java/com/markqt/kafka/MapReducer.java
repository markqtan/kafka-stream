package com.markqt.kafka;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapReducer {
	public static void main(String[] args) {
		List<LocalDate> dates = Arrays.asList(LocalDate.now());
		dates.forEach(d -> {System.out.println(d);});
		Function<LocalDate, String> addDate =
				(date) -> "The Day of the week is " + date.getDayOfWeek();
		List<String> strings = dates.stream().map(addDate).collect(Collectors.toList());
		System.out.println(strings);
		
		List<Integer> numbers = Arrays.asList(1, 2, 3);
		int sum = numbers.stream().reduce(0, (i, j) -> i + j );
		System.out.println(sum);
		
	}
}
