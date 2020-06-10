package com.mylp.sparkproject.spark.session;

import org.apache.spark.util.AccumulatorV2;

import com.mylp.sparkproject.constant.Constants;
import com.mylp.sparkproject.util.StringUtils;

public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {
	
	private static final long serialVersionUID = 1L;
	
	private String sessionAggrInfo = Constants.SESSION_COUNT + "=0|"
			+ Constants.TIME_PERIOD_1s_3s + "=0|"
			+ Constants.TIME_PERIOD_4s_6s + "=0|"
			+ Constants.TIME_PERIOD_7s_9s + "=0|"
			+ Constants.TIME_PERIOD_10s_30s + "=0|"
			+ Constants.TIME_PERIOD_30s_60s + "=0|"
			+ Constants.TIME_PERIOD_1m_3m + "=0|"
			+ Constants.TIME_PERIOD_3m_10m + "=0|"
			+ Constants.TIME_PERIOD_10m_30m + "=0|"
			+ Constants.TIME_PERIOD_30m + "=0|"
			+ Constants.STEP_PERIOD_1_3 + "=0|"
			+ Constants.STEP_PERIOD_4_6 + "=0|"
			+ Constants.STEP_PERIOD_7_9 + "=0|"
			+ Constants.STEP_PERIOD_10_30 + "=0|"
			+ Constants.STEP_PERIOD_30_60 + "=0|"
			+ Constants.STEP_PERIOD_60 + "=0";

	@Override
	public void add(String v) {
		if (StringUtils.isEmpty(sessionAggrInfo)) {
			return;
		}
		
		String oldValue = StringUtils.getFieldFromConcatString(
				sessionAggrInfo, "\\|", v);
		if (oldValue != null) {
			int newValue = Integer.valueOf(oldValue) + 1;
			sessionAggrInfo = StringUtils.setFieldInConcatString(sessionAggrInfo, "\\|", v, String.valueOf(newValue));
		}
	}

	@Override
	public AccumulatorV2<String, String> copy() {
		SessionAggrStatAccumulator accumulator = new SessionAggrStatAccumulator();
		accumulator.sessionAggrInfo = this.sessionAggrInfo;
		return accumulator;
	}

	@Override
	public boolean isZero() {
		return sessionAggrInfo.compareTo(Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0") == 0;
	}

	/**
	 * spark 累加器根据任务数量，会拷贝对应任务个数的累加器，在任务结束后，进行merge
	 */
	@Override
	public void merge(AccumulatorV2<String, String> other) {
		if (other instanceof SessionAggrStatAccumulator) {
			SessionAggrStatAccumulator otherAcc = (SessionAggrStatAccumulator) other;
			if (otherAcc.isZero()) {
				return;
			}
			else if (isZero()) {
				this.sessionAggrInfo = otherAcc.sessionAggrInfo;
			} else {
				// 两个累加器都不为zero的情况
				final String[] paramStrings = {
						Constants.SESSION_COUNT,
						Constants.TIME_PERIOD_1s_3s,
						Constants.TIME_PERIOD_4s_6s,
						Constants.TIME_PERIOD_7s_9s,
						Constants.TIME_PERIOD_10s_30s,
						Constants.TIME_PERIOD_30s_60s,
						Constants.TIME_PERIOD_1m_3m,
						Constants.TIME_PERIOD_3m_10m,
						Constants.TIME_PERIOD_10m_30m,
						Constants.TIME_PERIOD_30m,
						Constants.STEP_PERIOD_1_3,
						Constants.STEP_PERIOD_4_6,
						Constants.STEP_PERIOD_7_9,
						Constants.STEP_PERIOD_10_30,
						Constants.STEP_PERIOD_30_60,
						Constants.STEP_PERIOD_60
				};
				
				for(String paramString : paramStrings) {
					String otherValue = StringUtils.getFieldFromConcatString(
							otherAcc.sessionAggrInfo, "\\|", paramString);
					String selfValue = StringUtils.getFieldFromConcatString(
							sessionAggrInfo, "\\|", paramString);
					if (otherValue != null) {
						int newValue = Integer.valueOf(otherValue) + Integer.valueOf(selfValue);
						sessionAggrInfo = StringUtils.setFieldInConcatString(
								sessionAggrInfo, "\\|", paramString, String.valueOf(newValue));
					}
				}
			}
		}
	}

	@Override
	public void reset() {
		sessionAggrInfo = Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public String value() {
		return sessionAggrInfo;
	}

}
