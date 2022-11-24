require(ggplot2)
require(dplyr)
require(tidyr)
require(scales) # For the percent_format() function
require(stringr)

data <- read.csv('lane_activity.csv')


data_grouped <- data %>% group_by(iteration) %>%
    summarise_at(.vars = vars(discovered_cumulative,
                              discovered_iteration,
                              finished_iteration,
                              finished_total),
               .funs = c(mean="mean"))
data_grouped

data_long <- pivot_longer(data_grouped,
                          cols=c('discovered_cumulative_mean', "discovered_iteration_mean", "finished_iteration_mean", "finished_total_mean"),
                          names_to="category",
                          values_to="value")

data_long$iteration <- as.factor(data_long$iteration)


ggplot(data_long %>% filter(category == "discovered_cumulative_mean" | category == "discovered_iteration_mean"),
       aes(x=iteration, y=value, fill=category)) +
  geom_point(aes(col=category, group=category), size=2.5) +
  geom_line(aes(col=category, group=category)) +
  theme_classic()