package models;

import play.db.ebean.Model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name = "check_point")
public class CheckPoint extends Model {

    public static class TABLE {
        public static final String TABLE_NAME = "check_point";
    }

    @Id
    @Column(nullable = false)
    public long lastTime;

    public static Finder<String, CheckPoint> find = new Finder<String, CheckPoint>(String.class, CheckPoint.class);
}