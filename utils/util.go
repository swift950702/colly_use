package util

import (
	"strconv"
	"strings"
)

func IpToBite(ip string) (res int64) {
	dot := strings.Split(ip, ".")
	for k, v := range dot {
		i, _ := strconv.Atoi(v)
		res += int64(i * 1 << (24 - k*8))
	}
	return
}

func BinarySearch(ips []int64, ip int64) int {
	low, high := 0, len(ips)-1
	var mid int
	for low <= high {
		mid = (low + high) / 2
		if ips[mid] < ip {
			low = mid + 1
		} else if ips[mid] > ip {
			high = mid - 1
		} else {
			return mid
		}
	}
	return -1
}

// 快排 不稳定 O(n*log2(n))
func QuickSort(ips []int64, start int, end int) {
	if start < end {
		temp := ips[start]
		i, j := start, end
		for i != j {
			for j > i && ips[j] >= temp {
				j--
			}
			ips[i] = ips[j]
			for j > i && ips[i] <= temp {
				i++
			}
			ips[j] = ips[i]
		}
		ips[i] = temp
		QuickSort(ips, start, i-1)
		QuickSort(ips, i+1, end)
	}
}
