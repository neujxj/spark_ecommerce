package com.mylp.sparkproject.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 品类二次排序key，根据点击次数、下单次数、支付次数依次排序
 * @author Administrator
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable {

	private static final long serialVersionUID = 1L;
	
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		super();
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
	public int compare(CategorySortKey that) {
		if (clickCount - that.getClickCount() != 0) {
			return (int) (clickCount - that.getClickCount());
		} else if (orderCount - that.getOrderCount() != 0) {
			return (int) (orderCount - that.getOrderCount());
		} else if (payCount - that.getPayCount() != 0) {
			return (int) (payCount - that.getPayCount());
		}
		return 0;
	}
	
	@Override
	public int compareTo(CategorySortKey that) {
		if (clickCount - that.getClickCount() != 0) {
			return (int) (clickCount - that.getClickCount());
		} else if (orderCount - that.getOrderCount() != 0) {
			return (int) (orderCount - that.getOrderCount());
		} else if (payCount - that.getPayCount() != 0) {
			return (int) (payCount - that.getPayCount());
		}
		return 0;
	}

}
