const DEFAULT_ANALYSIS_RANGE_MAX_DAYS = 30;

const parseMaxDays = (value) => {
  const parsed = Number.parseInt(value, 10);
  if (Number.isFinite(parsed) && parsed >= 0) {
    return parsed;
  }
  return DEFAULT_ANALYSIS_RANGE_MAX_DAYS;
};

export const ANALYSIS_RANGE_MAX_DAYS = parseMaxDays(
  import.meta.env.VITE_ANALYSIS_RANGE_MAX_DAYS,
);
