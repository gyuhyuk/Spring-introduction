package hello.hellospring.repository;

import hello.hellospring.domain.Member;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class JdbcMemberRepository implements MemberRepository{

    private final DataSource dataSource;

    public JdbcMemberRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Member save(Member member) {
        String sql = "insert into member(name) values(?)";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null; // 결과를 받는 것

        try {
            conn = getConnection(); // connection을 가지고 오는 것
            pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS); // connection에서 prepare statement에서 sql을 넣고
            // RETURN_GENERATE_KEYS는 DB에 insert하면 insert를 해봐야 id 값 1번, 2번 ...을 얻을 수 있다
            pstmt.setString(1, member.getName()); // parameterIndex 1번이라고 하면 위 sql의 values (?)의 ?와 매칭이 된다. 거기에 member.getName으로 값을 넣는다.
            pstmt.executeUpdate(); // 이 때 DB의 실제 query가 날라간다. (insert into query) - executeUpdate
            rs = pstmt.getGeneratedKeys(); // 위의 RETURN_GENERATED_KEYS와 매칭됨 (만약 1번을 return 해주면 1번을 return)
            if (rs.next()) { // result set.next 해서 값이 있으면 값을 꺼내면 된다
                member.setId(rs.getLong(1)); // getLong으로 값을 꺼낸다
            } else {
                throw new SQLException("id 조회 실패"); // 실패
            }
            return member;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            close(conn, pstmt, rs); // 데이터베이스 커넥션 끊음으로써 리소스 반환
        }
    }
    @Override
    public Optional<Member> findById(Long id) { // 조회
        String sql = "select * from member where id = ?"; // sql 날려서 가지고오기
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection(); // 커넥션 연결
            pstmt = conn.prepareStatement(sql); // sql 날리기
            pstmt.setLong(1, id); // prepare statement 세팅
            rs = pstmt.executeQuery(); // 조회는 executeQuery
            if(rs.next()) { // 만약 result set을 받아와서 값이 존재하면
                Member member = new Member(); // 멤버 객체를 만든다
                member.setId(rs.getLong("id"));
                member.setName(rs.getString("name"));
                return Optional.of(member); // 멤버 객체 반환
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            close(conn, pstmt, rs);
        } }
    @Override
    public List<Member> findAll() { // 전부 조회
        String sql = "select * from member";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            List<Member> members = new ArrayList<>(); // List Collection에 담는다.
            while(rs.next()) { // 루프 돌리면서
                Member member = new Member();
                member.setId(rs.getLong("id"));
                member.setName(rs.getString("name"));
                members.add(member); // Collection.add 해서 멤버를 쭉 담는다.
            }
            return members; // 멤버 반환
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            close(conn, pstmt, rs);
        }
    }
    @Override
    public Optional<Member> findByName(String name) {
        String sql = "select * from member where name = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, name);
            rs = pstmt.executeQuery();
            if(rs.next()) {
                Member member = new Member();
                member.setId(rs.getLong("id"));
                member.setName(rs.getString("name"));
                return Optional.of(member);
            }
            return Optional.empty(); // 없으면 empty 반환
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            close(conn, pstmt, rs);
        }
    }
    private Connection getConnection() { // Spring Framework를 쓸 때는 꼭 이렇게 가지고 와야함.
        return DataSourceUtils.getConnection(dataSource);
    }
    private void close(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if (conn != null) {
                close(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void close(Connection conn) throws SQLException { // 닫을 때
        DataSourceUtils.releaseConnection(conn, dataSource); // DataSourceUtils를 통해서 release를 해줘야 한다.
    }
}
