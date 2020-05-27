package cn;

public class UserBehavior {
    private long userId;         // 用户ID
    private long itemId;         // 商品ID
    private int categoryId;      // 商品类目ID
    private String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
    private long timestamp;      // 行为发生的时间戳，单位秒

    public long getUserId() { return userId; }
    public long getItemId() { return itemId; }
    public int getCategoryId() { return categoryId; }
    public String getBehavior() { return behavior; }
    public long getTimestamp() { return timestamp; }

    public void setUserId(long userId) { this.userId = userId; }
    public void setItemId(long itemId) { this.itemId = itemId; }
    public void setCategoryId(int categoryId) { this.categoryId = categoryId; }
    public void setBehavior(String behavior) { this.behavior = behavior; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public static UserBehavior parseString(String s){
        System.out.println(s);
        UserBehavior userBehavior = new UserBehavior();
        String[] data = s.split(",");
        userBehavior.setUserId(Long.valueOf(data[0]).longValue());
        userBehavior.setItemId(Long.valueOf(data[1]).longValue());
        userBehavior.setCategoryId(Integer.valueOf(data[2]).intValue());
        userBehavior.setBehavior(data[3]);
        userBehavior.setTimestamp(Long.valueOf(data[4]).longValue() * 1000);
        return userBehavior;
    }
}
